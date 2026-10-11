"""Creación única de la SparkSession a partir de la configuración.

POR QUÉ centralizarla: el mismo código debe correr en modo local (laptop) y
sobre YARN (clúster del curso); solo cambia `spark.master` en el YAML.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from src.config import load_config


def get_spark(cfg: dict | None = None, app_suffix: str = "") -> SparkSession:
    cfg = cfg or load_config()
    sc = cfg["spark"]
    return (
        SparkSession.builder.master(sc["master"])
        .appName(sc["app_name"] + (f"-{app_suffix}" if app_suffix else ""))
        .config("spark.driver.memory", sc["driver_memory"])
        # 200 particiones de shuffle (valor por defecto) generan cientos de
        # archivos diminutos para un volumen de ~165 MB; se ajusta en el YAML.
        .config("spark.sql.shuffle.partitions", sc["shuffle_partitions"])
        # Fechas en hora de Perú para que anio/mes no se desplacen por zona horaria.
        .config("spark.sql.session.timeZone", "America/Lima")
        .getOrCreate()
    )
