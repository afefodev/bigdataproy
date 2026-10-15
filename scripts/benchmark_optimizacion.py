"""Paso 6 — Optimización del rendimiento medida antes/después (sección 3.6).

Tres optimizaciones, cada una medida con la MISMA consulta y los MISMOS datos
reales, repitiendo varias veces y reportando la mediana (la primera ejecución
de cada caso se descarta como "calentamiento" de la JVM y del caché del disco).

  O1. Formato y particionado: consulta analítica sobre los CSV crudos vs. sobre
      el Parquet particionado por anio/mes (poda de particiones + de columnas).
  O2. Expresiones de limpieza: versión inicial (regex por columna, probar todos
      los formatos de fecha, cadena de withColumn) vs. versión optimizada
      (btrim, formato elegido por patrón, un select por etapa).
  O3. Join con tabla pequeña: sort-merge join (shuffle de ambas tablas) vs.
      broadcast join (la tabla pequeña se copia a cada ejecutor).

Uso: .venv/bin/python -m scripts.benchmark_optimizacion [--repeticiones 5]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.io_utils import dir_size_bytes, get_logger, read_csv_dicts, write_csv_atomic
from src.config import REPO_ROOT, load_config, zone_path
from src.limpieza import fuente1_limpieza as L
from src.spark_session import get_spark

log = get_logger("benchmark", REPO_ROOT / "logs" / "benchmark_optimizacion.log")


def medir_par(nombre_a: str, accion_a, nombre_b: str, accion_b, repeticiones: int) -> tuple[dict, dict]:
    """Mide "antes" (a) y "después" (b) INTERCALADOS: a, b, b, a, a, b, ...

    POR QUÉ intercalar: la máquina de desarrollo es un portátil compartido con
    otras aplicaciones (carga variable) y sin ventilador (puede bajar su
    frecuencia con el calor). Si se midiera todo "a" y luego todo "b", cualquier
    cambio de carga favorecería a uno de los dos. Alternando el orden, el ruido
    afecta a ambos por igual. Se reporta la mediana, que es robusta a picos.
    """
    accion_a(); accion_b()  # calentamiento (no se cuenta)
    ta, tb = [], []
    carga = []
    for i in range(repeticiones):
        orden = [(accion_a, ta), (accion_b, tb)] if i % 2 == 0 else [(accion_b, tb), (accion_a, ta)]
        for accion, lista in orden:
            t = time.perf_counter()
            accion()
            lista.append(time.perf_counter() - t)
        carga.append(os.getloadavg()[0])
    out = []
    for nombre, tiempos in [(nombre_a, ta), (nombre_b, tb)]:
        r = {"caso": nombre, "repeticiones": repeticiones, "mediana_s": round(statistics.median(tiempos), 3),
             "min_s": round(min(tiempos), 3), "max_s": round(max(tiempos), 3),
             "carga_sistema_1min": round(statistics.median(carga), 1)}
        log.info("%-55s mediana %.3f s (min %.3f, max %.3f) carga %.1f", nombre, r["mediana_s"], r["min_s"],
                 r["max_s"], r["carga_sistema_1min"])
        out.append(r)
    return out[0], out[1]


def noop(df: DataFrame):
    # El formato "noop" ejecuta todo el plan sin escribir a disco: mide solo cómputo.
    return lambda: df.write.mode("overwrite").format("noop").save()


# --- O2: versión INICIAL de la normalización (la que se reemplazó), conservada
#     aquí solo para poder medir el "antes" con el código original.
def normalizar_v0(df: DataFrame) -> DataFrame:
    for _, c in L.COLUMNAS:
        v = F.regexp_replace(F.regexp_replace(F.col(c), r"^[\s ]+|[\s ]+$", ""), r"\s{2,}", " ")
        df = df.withColumn(c, F.when(v == "", None).otherwise(v))
    for c in L.CAMPOS_FECHA:
        df = df.withColumn(c, F.coalesce(*[F.to_timestamp(F.col(c), f) for f in L.FORMATOS_FECHA]))
    for c in L.CAMPOS_MONTO:
        df = df.withColumn(c, F.col(c).cast("decimal(18,2)"))
    df = df.withColumn("tipo_procedimiento", L.sin_tildes_mayus(F.col("tipo_procedimiento")))
    df = df.withColumn("estado_orden", F.upper(F.col("estado_orden")))
    patron = r"^((?:EXT|IM)-CE-\d{4}-\d+)\s+(.+)$"
    df = (df.withColumn("codigo_acuerdo_marco", F.when(F.col("acuerdo_marco_original").rlike(patron),
                                                       F.regexp_extract("acuerdo_marco_original", patron, 1)))
            .withColumn("acuerdo_marco", L.sin_tildes_mayus(
                F.when(F.col("acuerdo_marco_original").rlike(patron),
                       F.regexp_extract("acuerdo_marco_original", patron, 2)).otherwise(F.col("acuerdo_marco_original")))))
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeticiones", type=int, default=5)
    args = ap.parse_args()
    cfg = load_config()
    spark = get_spark(cfg, "benchmark")
    ruc = cfg["entidad_cliente"]["ruc"]
    raw_root = Path(zone_path(cfg, "raw", "ordenes_compra"))
    proc_path = zone_path(cfg, "processed", "ordenes_compra", "v1")
    manifest = read_csv_dicts(raw_root / "_manifiesto.csv")
    resultados = []

    # ---------------------------------------------------------------- O1
    # Pregunta de negocio: ¿cuánto compró la MPT en 2025 por acuerdo marco?
    def consulta(df):
        return (df.where((F.col("ruc_entidad") == ruc) & (F.col("anio") == 2025))
                  .groupBy("acuerdo_marco").agg(F.count("*").alias("ordenes"), F.sum("total").alias("monto"),
                                                F.expr("percentile_approx(total, 0.5)").alias("mediana")))

    def desde_csv():
        crudo = L.leer_crudo(spark, raw_root, manifest)
        df = L.normalizar(crudo).withColumn("anio", F.year("fecha_formalizacion"))
        return consulta(df)

    def desde_parquet():
        return consulta(spark.read.parquet(proc_path))

    r1, r2 = medir_par("O1-antes: CSV crudo (57 archivos, todas las columnas)", noop(desde_csv()),
                       "O1-después: Parquet particionado (poda anio=2025)", noop(desde_parquet()), args.repeticiones)
    plan = desde_parquet()._jdf.queryExecution().executedPlan().toString()
    poda = bool(re.search(r"PartitionFilters: \[[^\]]*anio", plan))
    bytes_2025 = dir_size_bytes(Path(proc_path) / "anio=2025")
    r1.update({"optimizacion": "O1", "bytes_leidos": sum(int(m["bytes"]) for m in manifest)})
    r2.update({"optimizacion": "O1", "bytes_leidos": bytes_2025, "poda_de_particiones_en_plan": poda})
    # Mismo resultado en ambos caminos (si no, la comparación no vale).
    a = {r["acuerdo_marco"]: (r["ordenes"], str(r["monto"])) for r in desde_csv().collect()}
    b = {r["acuerdo_marco"]: (r["ordenes"], str(r["monto"])) for r in desde_parquet().collect()}
    r2["mismo_resultado_que_antes"] = a == b
    resultados += [r1, r2]

    # ---------------------------------------------------------------- O2
    crudo = L.leer_crudo(spark, raw_root, manifest).cache()
    crudo.count()  # se cachea la lectura para medir SOLO las expresiones
    r3, r4 = medir_par("O2-antes: normalización inicial (regex + withColumn)", noop(L.derivar(normalizar_v0(crudo), ruc)),
                       "O2-después: normalización optimizada (btrim + select)", noop(L.derivar(L.normalizar(crudo), ruc)),
                       args.repeticiones)
    r3["optimizacion"] = r4["optimizacion"] = "O2"
    va = L.derivar(normalizar_v0(crudo), ruc).select("orden_electronica", "fecha_formalizacion", "acuerdo_marco", "total")
    vb = L.derivar(L.normalizar(crudo), ruc).select("orden_electronica", "fecha_formalizacion", "acuerdo_marco", "total")
    r4["mismo_resultado_que_antes"] = va.exceptAll(vb).count() == 0 and vb.exceptAll(va).count() == 0
    crudo.unpersist()
    resultados += [r3, r4]

    # ---------------------------------------------------------------- O3
    # Tabla pequeña real: estadísticos por acuerdo marco y tipo de procedimiento
    # (60 filas) unidos a las 511 782 órdenes, p. ej. para calcular la razón
    # monto / mediana del grupo.
    ordenes = spark.read.parquet(proc_path).select("orden_electronica", "acuerdo_marco", "tipo_procedimiento", "total_double")
    stats = (ordenes.groupBy("acuerdo_marco", "tipo_procedimiento")
                    .agg(F.expr("percentile_approx(total_double, 0.5)").alias("mediana_grupo"))).cache()
    n_stats = stats.count()
    llave = ["acuerdo_marco", "tipo_procedimiento"]
    # Para comparar las dos estrategias en las mismas condiciones se desactiva
    # la elección automática (y la de AQE) y se fuerza cada una con una pista.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    smj = ordenes.join(stats.hint("merge"), llave).withColumn("ratio", F.col("total_double") / F.col("mediana_grupo"))
    bj = ordenes.join(F.broadcast(stats), llave).withColumn("ratio", F.col("total_double") / F.col("mediana_grupo"))
    r5, r6 = medir_par("O3-antes: sort-merge join (shuffle de ambas tablas)", noop(smj),
                       "O3-después: broadcast join (tabla de %d filas)" % n_stats, noop(bj), args.repeticiones)
    plan_smj = smj._jdf.queryExecution().executedPlan().toString()
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.unset("spark.sql.adaptive.autoBroadcastJoinThreshold")
    r5["optimizacion"] = r6["optimizacion"] = "O3"
    r5["estrategia_en_plan"] = "SortMergeJoin" if "SortMergeJoin" in plan_smj else "?"
    r6["estrategia_en_plan"] = "BroadcastHashJoin" if "BroadcastHashJoin" in bj._jdf.queryExecution().executedPlan().toString() else "?"
    resultados += [r5, r6]

    for antes, despues in [(r1, r2), (r3, r4), (r5, r6)]:
        despues["aceleracion_x"] = round(antes["mediana_s"] / despues["mediana_s"], 2)

    campos = ["optimizacion", "caso", "repeticiones", "mediana_s", "min_s", "max_s", "aceleracion_x",
              "carga_sistema_1min", "bytes_leidos",
              "poda_de_particiones_en_plan", "mismo_resultado_que_antes", "estrategia_en_plan"]
    write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "optimizacion.csv", resultados, campos)
    (REPO_ROOT / "docs" / "evidencias" / "optimizacion_plan_parquet.txt").write_text(plan, encoding="utf-8")
    log.info("Resultados: %s", json.dumps(resultados, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
