"""Motor genérico de reglas de calidad declaradas en YAML (Paso 5, sección 3.4).

El motor no conoce ninguna regla concreta: lee el YAML, evalúa cada expresión
SQL como una columna booleana y separa los registros en válidos y cuarentena.
Así, agregar o cambiar una regla no exige tocar código.
"""
from __future__ import annotations

from datetime import datetime, timezone

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def cargar_reglas(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def aplicar_reglas(df: DataFrame, reglas: list[dict]) -> DataFrame:
    """Agrega una columna booleana `_ok_<id>` por regla.

    `coalesce(expr, false)`: si la expresión da NULL (p. ej. comparar contra un
    monto nulo) se cuenta como incumplimiento; un dato desconocido no se da por
    bueno.
    """
    for r in reglas:
        df = df.withColumn(f"_ok_{r['id']}", F.coalesce(F.expr(r["expresion"]), F.lit(False)))
    rech = [r["id"] for r in reglas if r["severidad"] == "rechazo"]
    adv = [r["id"] for r in reglas if r["severidad"] == "advertencia"]

    def fallidas(ids):
        # array con los id de las reglas incumplidas (vacío si cumple todas)
        return F.filter(F.array(*[F.when(~F.col(f"_ok_{i}"), F.lit(i)) for i in ids]), lambda x: x.isNotNull()) \
            if ids else F.array().cast("array<string>")

    return (df.withColumn("motivos_rechazo", fallidas(rech))
              .withColumn("alertas_calidad", fallidas(adv)))


def resumen_reglas(df_eval: DataFrame, reglas: list[dict], total: int) -> list[dict]:
    """Cuántos registros incumplen cada regla (una sola pasada de agregación)."""
    aggs = [F.sum(F.when(~F.col(f"_ok_{r['id']}"), 1).otherwise(0)).alias(r["id"]) for r in reglas]
    fila = df_eval.agg(*aggs).collect()[0].asDict()
    out = []
    for r in reglas:
        n = int(fila[r["id"]] or 0)
        out.append({"regla": r["id"], "severidad": r["severidad"], "descripcion": r["descripcion"],
                    "registros_evaluados": total, "incumplen": n,
                    "pct_incumplen": round(100 * n / total, 4) if total else 0.0,
                    "resultado": "OK" if n == 0 else ("RECHAZADOS" if r["severidad"] == "rechazo" else "MARCADOS")})
    return out


def completitud(df: DataFrame, campos: list[str], etapa: str) -> list[dict]:
    """% de valores no nulos por campo crítico (evidencia de la V de veracidad)."""
    total = df.count()
    fila = df.agg(*[F.count(F.col(c)).alias(c) for c in campos]).collect()[0].asDict()
    return [{"etapa": etapa, "campo": c, "registros": total, "no_nulos": int(fila[c]),
             "pct_completitud": round(100 * fila[c] / total, 3) if total else 0.0} for c in campos]


def sello_tiempo() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
