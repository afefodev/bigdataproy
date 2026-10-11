"""Creación única de la SparkSession a partir de la configuración.

POR QUÉ centralizarla: el mismo código debe correr en modo local (laptop) y
sobre YARN (clúster del curso); solo cambia `spark.master` en el YAML.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from src.config import load_config


def get_spark(cfg: dict | None = None, app_suffix: str = "") -> SparkSession:
    cfg = cfg or load_config()
    sc = cfg["spark"]
    # Los workers de Python deben usar el MISMO intérprete que el driver (el del
    # .venv); si no, toman el python3 del sistema y Spark aborta por versión.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master(sc["master"])
        .appName(sc["app_name"] + (f"-{app_suffix}" if app_suffix else ""))
        .config("spark.driver.memory", sc["driver_memory"])
        # 200 particiones de shuffle (valor por defecto) generan cientos de
        # archivos diminutos para un volumen de ~165 MB; se ajusta en el YAML.
        .config("spark.sql.shuffle.partitions", sc["shuffle_partitions"])
        # Fechas en hora de Perú para que anio/mes no se desplacen por zona horaria.
        .config("spark.sql.session.timeZone", "America/Lima")
        # Parser de fechas moderno: un valor que no calza con el formato da NULL
        # (y luego cae en una regla de calidad) en vez de lanzar una excepción.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    # Solo errores en consola: los avisos de cabecera del lector CSV son
    # esperados (la cabecera se valida explícitamente en la limpieza).
    spark.sparkContext.setLogLevel("ERROR")
    return spark
