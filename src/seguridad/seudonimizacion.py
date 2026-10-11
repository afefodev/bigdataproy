"""Paso 10 — Seudonimización de proveedores persona natural (sección 3.8 y 5.1).

Problema: el RUC de una persona natural empieza en 10 (y, con menos frecuencia,
en 15 o 17) y contiene su DNI (RUC 10 = '10' + DNI + dígito verificador). Su
"razón social" es directamente su nombre. Ambos son datos personales según la
Ley N.° 29733, aunque Perú Compras los publique.

Técnica: HMAC-SHA256 con una clave secreta (la "sal") que vive en una variable
de entorno y nunca en el repositorio.
* Determinista: el mismo RUC siempre da el mismo seudónimo, así el análisis de
  concentración de proveedores sigue funcionando sin saber quién es quién.
* No reversible sin la clave: un atacante no puede probar los ~30 millones de
  DNI posibles y comparar hashes, porque sin la clave no puede calcularlos.
  (Un hash SIN clave de un DNI sí sería reversible por fuerza bruta: por eso
  no se usa sha256(ruc) a secas.)
* HMAC y no sha256(sal + ruc): es la construcción estándar para hash con clave
  y además la clave no queda escrita como literal en el plan de Spark/logs.

Las personas jurídicas (RUC 20) no se seudonimizan: su razón social no es dato
personal y es necesaria para el análisis de concentración.
"""
from __future__ import annotations

import hashlib
import hmac
import os

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

PREFIJOS_PERSONA_NATURAL = ("10", "15", "17")
LONGITUD_TOKEN = 16  # 64 bits: sin colisiones prácticas para ~80 mil proveedores


def obtener_clave(nombre_variable: str) -> bytes:
    clave = os.environ.get(nombre_variable, "")
    if len(clave) < 32:
        # Fallar es preferible a seudonimizar con una clave vacía o débil.
        raise RuntimeError(f"Defina {nombre_variable} (>= 32 caracteres) en .env antes de construir consumo")
    return clave.encode("utf-8")


def token(ruc: str, clave: bytes) -> str:
    return "PN-" + hmac.new(clave, ruc.encode("utf-8"), hashlib.sha256).hexdigest()[:LONGITUD_TOKEN]


def udf_seudonimo(clave: bytes):
    @pandas_udf("string")
    def _seud(rucs: pd.Series) -> pd.Series:
        # pandas_udf procesa lotes de filas vía Arrow (mucho más rápido que una UDF fila a fila).
        return rucs.map(lambda r: None if r is None else token(r, clave))
    return _seud


def seudonimizar_proveedor(ruc_col: str, nombre_col: str, clave: bytes) -> tuple[Column, Column]:
    """Devuelve (ruc_proveedor_publico, proveedor_publico)."""
    es_pn = F.substring(F.col(ruc_col), 1, 2).isin(*PREFIJOS_PERSONA_NATURAL)
    seud = udf_seudonimo(clave)(F.col(ruc_col))
    ruc_pub = F.when(es_pn, seud).otherwise(F.col(ruc_col))
    nom_pub = F.when(es_pn, F.concat(F.lit("PERSONA NATURAL "), seud)).otherwise(F.col(nombre_col))
    return ruc_pub, nom_pub
