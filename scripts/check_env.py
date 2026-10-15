"""Paso 1 — verificación del entorno.

Imprime versiones reales y ejecuta una consulta mínima en Spark que escribe y
lee Parquet en la zona de pruebas. Si esto pasa, el entorno está operativo.
Uso:  .venv/bin/python -m scripts.check_env
"""
import platform
import shutil
import sys
import tempfile
import time

import pyarrow
import pyspark

from src.config import load_config, base_path
from src.spark_session import get_spark

cfg = load_config()
print(f"Python  : {sys.version.split()[0]} ({platform.machine()})")
print(f"PySpark : {pyspark.__version__}")
print(f"PyArrow : {pyarrow.__version__}")
print(f"Lake    : {base_path(cfg)}")

t0 = time.perf_counter()
spark = get_spark(cfg, "check-env")
print(f"Java    : {spark.sparkContext._jvm.java.lang.System.getProperty('java.version')}")
print(f"Master  : {spark.sparkContext.master}")

tmp = tempfile.mkdtemp(prefix="pj_check_")
df = spark.range(1_000_000).selectExpr("id", "id % 12 + 1 AS mes")
df.write.mode("overwrite").partitionBy("mes").parquet(tmp)
n = spark.read.parquet(tmp).where("mes = 3").count()
print(f"Prueba  : escritura/lectura Parquet particionado OK ({n} filas en mes=3)")
print(f"Tiempo  : {time.perf_counter() - t0:.1f} s (incluye arranque de la JVM)")
spark.stop()
shutil.rmtree(tmp, ignore_errors=True)
