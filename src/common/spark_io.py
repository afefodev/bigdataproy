"""Escritura atómica de DataFrames, válida para disco local y para HDFS/S3.

POR QUÉ usar la API FileSystem de Hadoop (vía la JVM de Spark) y no `os.rename`:
la misma función funciona si `base_path` es local, `hdfs://` o `s3a://`, que es
justo lo que pide la prueba de sustitución de almacenamiento (sección 2.3).

Patrón: Spark escribe en `<destino>.__tmp`; solo si la escritura terminó sin
error se reemplaza el destino. Si el job se cae a la mitad, el destino anterior
queda intacto y basta con relanzar (idempotente: siempre `overwrite`).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def _fs_and_path(spark: SparkSession, path: str):
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs, hpath, jvm


def write_parquet_atomic(df: DataFrame, final_path: str, partition_by: list[str] | None = None,
                         num_files: int | None = None) -> None:
    spark = df.sparkSession
    tmp_path = final_path.rstrip("/") + ".__tmp"
    if num_files:
        # Pocos archivos por partición: evita el "problema de archivos pequeños"
        # (miles de Parquet diminutos que hacen lenta la lectura).
        df = df.repartition(num_files, *partition_by) if partition_by else df.coalesce(num_files)
    w = df.write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.parquet(tmp_path)

    fs, final, jvm = _fs_and_path(spark, final_path)
    tmp = jvm.org.apache.hadoop.fs.Path(tmp_path)
    old = jvm.org.apache.hadoop.fs.Path(final_path.rstrip("/") + ".__old")
    if fs.exists(old):
        fs.delete(old, True)
    if fs.exists(final):
        fs.rename(final, old)
    fs.mkdirs(final.getParent())
    if not fs.rename(tmp, final):
        raise RuntimeError(f"No se pudo renombrar {tmp_path} -> {final_path}")
    if fs.exists(old):
        fs.delete(old, True)


def write_text_atomic(spark: SparkSession, final_path: str, content: str) -> None:
    """Escribe un archivo de texto pequeño (p. ej. el esquema versionado) en el lake."""
    fs, final, jvm = _fs_and_path(spark, final_path)
    tmp = jvm.org.apache.hadoop.fs.Path(final_path + ".tmp")
    out = fs.create(tmp, True)
    out.write(bytearray(content.encode("utf-8")))
    out.close()
    if fs.exists(final):
        fs.delete(final, False)
    fs.rename(tmp, final)
