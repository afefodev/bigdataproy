"""Capa de consumo: tablas listas para el EDA, el modelo (sección 4) y el tablero.

Reglas de esta capa (contrato con los consumidores):
* Ningún dato personal: proveedores persona natural seudonimizados (HMAC).
* Sin columnas de enlaces a PDF: esos documentos contienen nombres, DNI,
  correos y teléfonos de representantes y servidores públicos.
* El filtro de la entidad cliente (MPT) se aplica AQUÍ, no antes: el modelo se
  entrena con el histórico nacional y la alerta se consulta para la MPT.

Tablas:
  consumo/ordenes_nacional   todas las entidades (entrenamiento del modelo), particionada por anio
  consumo/ordenes_mpt        solo la Municipalidad Provincial de Trujillo (tablero / alerta)
  consumo/items_orden_mpt    detalle por ítem desde los PDF (si ya se ejecutó la Fuente 2b)

Uso: .venv/bin/python -m src.consumo.construir_consumo
"""
from __future__ import annotations

import json
import time

from pyspark.sql import functions as F

from src.common.io_utils import get_logger
from src.common.spark_io import write_parquet_atomic
from src.config import REPO_ROOT, load_config, zone_path
from src.seguridad.seudonimizacion import obtener_clave, seudonimizar_proveedor
from src.spark_session import get_spark

COLUMNAS_CONSUMO = [
    "orden_electronica", "fecha_formalizacion", "fecha_formalizacion_dia", "fecha_ultimo_estado", "fecha_proceso",
    "ruc_entidad", "entidad", "es_mpt", "ruc_proveedor", "proveedor", "tipo_persona_proveedor",
    "tipo_procedimiento", "acuerdo_marco", "codigo_acuerdo_marco", "estado_orden", "descripcion_estado",
    "flag_estado_adverso", "flag_pagada", "flag_documento_estado", "sub_total", "igv", "total", "total_double",
    "log10_total", "tasa_igv", "flag_igv_cero", "dias_formalizacion_a_ultimo_estado",
    "dias_observacion_hasta_corte", "dia_semana", "dia_mes", "alertas_calidad", "anio", "mes",
]


def main() -> dict:
    cfg = load_config()
    log = get_logger("consumo", REPO_ROOT / "logs" / "consumo.log")
    clave = obtener_clave(cfg["seguridad"]["salt_env_var"])
    spark = get_spark(cfg, "consumo")
    t0 = time.perf_counter()
    try:
        proc = spark.read.parquet(zone_path(cfg, "processed", "ordenes_compra", "v1"))
        ruc_pub, nom_pub = seudonimizar_proveedor("ruc_proveedor", "proveedor", clave)
        # Ambas columnas se calculan en UNA proyección a partir del RUC original.
        # (Con dos withColumn seguidos, el segundo vería el RUC ya seudonimizado
        # y dejaría el nombre en claro: error detectado y corregido en pruebas.)
        reemplazos = {"ruc_proveedor": ruc_pub, "proveedor": nom_pub}
        nacional = proc.select(*[reemplazos.get(c, F.col(c)).alias(c) for c in COLUMNAS_CONSUMO])
        write_parquet_atomic(nacional, zone_path(cfg, "consumo", "ordenes_nacional"), partition_by=["anio"], num_files=5)
        nac = spark.read.parquet(zone_path(cfg, "consumo", "ordenes_nacional"))

        mpt = nac.where(F.col("es_mpt"))
        write_parquet_atomic(mpt, zone_path(cfg, "consumo", "ordenes_mpt"), num_files=1)

        # Verificación de la seudonimización: ningún RUC 10/15/17 en claro.
        fugas = nac.where(F.col("ruc_proveedor").rlike("^(10|15|17)[0-9]{9}$")
                          | ((F.col("tipo_persona_proveedor") == "NATURAL")
                             & ~F.col("proveedor").startswith("PERSONA NATURAL PN-"))).count()
        n_pn = nac.where(F.col("tipo_persona_proveedor") == "NATURAL").count()
        n_tokens = nac.where(F.col("tipo_persona_proveedor") == "NATURAL").select("ruc_proveedor").distinct().count()
        n_ruc_pn = proc.where(F.col("tipo_persona_proveedor") == "NATURAL").select("ruc_proveedor").distinct().count()

        res = {"ordenes_nacional": nac.count(), "ordenes_mpt": spark.read.parquet(zone_path(cfg, "consumo", "ordenes_mpt")).count(),
               "registros_persona_natural": n_pn, "ruc_pn_distintos": n_ruc_pn, "seudonimos_distintos": n_tokens,
               "colisiones": n_ruc_pn - n_tokens, "pn_en_claro": fugas}

        # Detalle por ítem (Fuente 2b), si existe: también sin datos personales.
        try:
            items = spark.read.parquet(zone_path(cfg, "processed", "items_orden_mpt"))
            write_parquet_atomic(items, zone_path(cfg, "consumo", "items_orden_mpt"), num_files=1)
            res["items_orden_mpt"] = items.count()
        except Exception:  # noqa: BLE001 — la Fuente 2b es opcional para esta etapa
            log.info("items_orden_mpt aún no existe en processed; se omite")

        res["segundos"] = round(time.perf_counter() - t0, 2)
        if fugas:
            raise RuntimeError(f"Seudonimización incompleta: {fugas} registros de persona natural en claro")
        log.info("RESUMEN consumo: %s", res)
        (REPO_ROOT / "docs" / "evidencias").mkdir(parents=True, exist_ok=True)
        (REPO_ROOT / "docs" / "evidencias" / "metricas_consumo.json").write_text(json.dumps(res, indent=1), encoding="utf-8")
        return res
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
