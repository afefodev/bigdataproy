"""Pasos 4 y 5 — Limpieza y control de calidad de la Fuente 1 (PySpark).

raw/ordenes_compra (CSV crudos, inmutables)
   └─> [lectura como TEXTO + verificación de cabecera contra el diccionario]
   └─> [normalización: espacios/tabs, fechas multiformato, montos, categorías]
   └─> [reglas de calidad declaradas en config/reglas_calidad_ordenes.yaml]
         ├─> processed/ordenes_compra/v1   (Parquet, particionado anio/mes)
         └─> cuarentena/ordenes_compra     (rechazados + motivo)
   └─> docs/evidencias/calidad_*.csv       (completitud y resultado de cada regla)

Decisiones (y por qué), para la sustentación:
* Todo se lee como texto (StringType). Si Spark infiriera tipos, una sola
  fecha en otro formato o un tab al inicio convertirían la columna entera en
  texto o en nulos sin avisar. Tipar a mano, columna por columna, deja rastro.
* La cabecera de cada archivo se compara con el diccionario oficial ANTES de
  leer: si Perú Compras cambia columnas, el job falla en vez de mezclar datos.
* Cada archivo se lee con la codificación detectada en la ingesta (Perú
  Compras mezcla cp1252 y UTF-8 según el mes).
* Nada se "arregla" inventando valores: un dato inválido va a cuarentena con
  su motivo; un dato raro pero posible se conserva y se marca.

Uso: .venv/bin/python -m src.limpieza.fuente1_limpieza
"""
from __future__ import annotations

import csv
import gzip
import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.calidad.motor_reglas import aplicar_reglas, cargar_reglas, completitud, resumen_reglas, sello_tiempo
from src.common.io_utils import get_logger, read_csv_dicts, write_csv_atomic
from src.common.spark_io import write_parquet_atomic, write_text_atomic
from src.config import REPO_ROOT, load_config, zone_path
from src.spark_session import get_spark

FUENTE = "ordenes_compra"
SCHEMA_VERSION = "v1"
SCHEMA_FILE = REPO_ROOT / "config" / "schemas" / f"ordenes_compra.{SCHEMA_VERSION}.json"
REGLAS_FILE = REPO_ROOT / "config" / "reglas_calidad_ordenes.yaml"

# Contrato con la fuente: (nombre en el CSV publicado, nombre interno snake_case).
# El CSV usa DOCUMENTO_ESTADO_OCAM aunque el diccionario oficial dice
# DOCUMENTO_ESTADO; se acepta el nombre real observado en los 57 archivos.
COLUMNAS = [
    ("FECHA_PROCESO", "fecha_proceso"),
    ("RUC_PROVEEDOR", "ruc_proveedor"),
    ("PROVEEDOR", "proveedor"),
    ("RUC_ENTIDAD", "ruc_entidad"),
    ("ENTIDAD", "entidad"),
    ("TIPO_PROCEDIMIENTO", "tipo_procedimiento"),
    ("ORDEN_ELECTRÓNICA", "orden_electronica"),
    ("ORDEN_ELECTRÓNICA_GENERADA", "url_orden_generada"),
    ("ESTADO_ORDEN_ELECTRÓNICA", "estado_orden"),
    ("DOCUMENTO_ESTADO_OCAM", "url_documento_estado"),
    ("FECHA_FORMALIZACIÓN", "fecha_formalizacion"),
    ("FECHA_ÚLTIMO_ESTADO", "fecha_ultimo_estado"),
    ("SUB_TOTAL", "sub_total"),
    ("IGV", "igv"),
    ("TOTAL", "total"),
    ("ORDEN_DIGITALIZADA", "url_orden_digitalizada"),
    ("DESCRIPCIÓN_ESTADO", "descripcion_estado"),
    ("DESCRIPCIÓN_CESIÓN_DERECHOS", "descripcion_cesion_derechos"),
    ("ACUERDO_MARCO", "acuerdo_marco_original"),
]
CAMPOS_FECHA = ["fecha_proceso", "fecha_formalizacion", "fecha_ultimo_estado"]
CAMPOS_MONTO = ["sub_total", "igv", "total"]
# Formatos de fecha observados en los datos reales (Paso 2):
#   'yyyy-MM-dd HH:mm:ss' en 56 archivos; 'd/MM/yyyy H:mm' en 2024-06 (sin segundos)
FORMATOS_FECHA = ["yyyy-MM-dd HH:mm:ss", "d/MM/yyyy H:mm"]
ESTADOS_ADVERSOS = ["RESUELTA", "ORDEN DE COMPRA NULA"]
# Prefijos de RUC de personas naturales (10: con DNI; 15 y 17: otros documentos).
RUC_PERSONA_NATURAL = ["10", "15", "17"]


# ----------------------------------------------------------------- lectura
def verificar_cabeceras(manifest: list[dict], raw_root: Path) -> None:
    """Contrato de esquema: la cabecera de CADA archivo debe ser la esperada."""
    esperada = [c for c, _ in COLUMNAS]
    for m in manifest:
        p = ruta_archivo(raw_root, m)
        opener = gzip.open if p.suffix == ".gz" else open
        with opener(p, "rt", encoding=m["codificacion"]) as f:
            cab = [c.strip() for c in f.readline().rstrip("\r\n").split(";")]
        if cab != esperada:
            raise ValueError(f"Cabecera inesperada en {p.name}: {cab}")


def ruta_archivo(raw_root: Path, m: dict) -> Path:
    return raw_root / f"anio={m['anio']}" / f"mes={m['mes']}" / m["nombre_archivo"]


def leer_crudo(spark: SparkSession, raw_root: Path, manifest: list[dict]) -> DataFrame:
    """Lee todos los CSV como texto, un grupo por codificación, y los une."""
    schema = StructType([StructField(n, StringType()) for _, n in COLUMNAS]
                        + [StructField("_corrupt_record", StringType())])
    por_cod: dict[str, list[str]] = {}
    for m in manifest:
        por_cod.setdefault(m["codificacion"], []).append(str(ruta_archivo(raw_root, m)))
    partes = []
    for cod, rutas in por_cod.items():
        partes.append(
            spark.read.option("header", True).option("sep", ";").option("encoding", cod)
            .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema).csv(rutas)
        )
    df = partes[0]
    for p in partes[1:]:
        df = df.unionByName(p)
    return (df.withColumn("archivo_origen", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
              .withColumn("periodo_archivo", F.concat_ws("-",
                          F.regexp_extract(F.input_file_name(), r"anio=(\d{4})", 1),
                          F.regexp_extract(F.input_file_name(), r"mes=(\d{2})", 1))))


# ----------------------------------------------------------- normalización
# Columnas de texto libre donde además se colapsan espacios dobles internos
# ('PORTÁTILES  Y ESCÁNERES'). En RUC, URL y montos no hace falta y el regex
# cuesta tiempo (medido: es la parte más cara de la limpieza).
COLUMNAS_TEXTO_LIBRE = {"proveedor", "entidad", "tipo_procedimiento", "estado_orden",
                        "descripcion_estado", "acuerdo_marco_original"}
ESPACIOS = " \t\r\n\u00a0"


def limpiar_texto(c: str):
    """Quita espacios y TABULADORES al inicio/fin (el archivo 2024-03 trae un
    tab delante de cada campo) y convierte el vacío en NULL.

    Se usa btrim (recorte por conjunto de caracteres) en vez de un regex: es
    varias veces más rápido y hace exactamente lo mismo.
    """
    v = F.btrim(F.col(c), F.lit(ESPACIOS))
    if c in COLUMNAS_TEXTO_LIBRE:
        v = F.regexp_replace(v, r"\s{2,}", " ")
    return F.when(F.length(v) == 0, None).otherwise(v)


def sin_tildes_mayus(col):
    return F.translate(F.upper(col), "ÁÉÍÓÚÜ", "AEIOUU")


def a_fecha(c: str):
    """Elige el formato según la forma del texto en lugar de "probar" todos.

    Formatos observados en los datos reales (Paso 2):
      'yyyy-MM-dd HH:mm:ss' en 56 archivos; 'd/MM/yyyy H:mm' en 2024-06.
    Probar un formato que no calza hace que Java lance y capture una excepción
    por celda, lo que medimos como el segundo costo más alto del job.
    """
    col = F.col(c)
    return (F.when(col.rlike(r"^\d{4}-\d{2}-\d{2} "), F.to_timestamp(col, FORMATOS_FECHA[0]))
             .when(col.rlike(r"^\d{1,2}/\d{1,2}/\d{4} "), F.to_timestamp(col, FORMATOS_FECHA[1])))


def normalizar(df: DataFrame) -> DataFrame:
    """Un solo `select` por etapa: una cadena larga de withColumn genera planes
    enormes que Spark analiza una y otra vez y puede impedir la generación de
    código compilado (whole-stage codegen)."""
    extra = [c for c in df.columns if c not in {n for _, n in COLUMNAS}]
    df = df.select(*[limpiar_texto(n).alias(n) for _, n in COLUMNAS], *extra)

    patron = r"^((?:EXT|IM)-CE-\d{4}-\d+)\s+(.+)$"
    am = F.col("acuerdo_marco_original")
    tiene_codigo = am.rlike(patron)
    tipadas = {
        # Fechas con el formato que corresponde a su forma.
        **{c: a_fecha(c) for c in CAMPOS_FECHA},
        # Montos: DECIMAL(18,2) como indica el diccionario (no float: evita
        # errores de redondeo binario al sumar soles y céntimos).
        **{c: F.col(c).cast("decimal(18,2)") for c in CAMPOS_MONTO},
        # Categorías a mayúsculas sin tildes: 'Compra ordinaria' -> 'COMPRA ORDINARIA'.
        "tipo_procedimiento": sin_tildes_mayus(F.col("tipo_procedimiento")),
        "estado_orden": F.upper(F.col("estado_orden")),
    }
    return df.select(
        *[tipadas.get(c, F.col(c)).alias(c) for c in df.columns],
        # ACUERDO_MARCO: 72 variantes reales para ~30 catálogos. Algunos meses
        # anteponen el código del acuerdo ('EXT-CE-2021-7 ÚTILES ...') y otros
        # no; además varían tildes y espacios. Se separa el código (versión del
        # acuerdo) del nombre normalizado (familia de bienes), que es la
        # categoría estable para comparar montos en el tiempo.
        F.when(tiene_codigo, F.regexp_extract(am, patron, 1)).alias("codigo_acuerdo_marco"),
        sin_tildes_mayus(F.when(tiene_codigo, F.regexp_extract(am, patron, 2)).otherwise(am)).alias("acuerdo_marco"),
    )


# --------------------------------------------------------------- derivadas
def derivar(df: DataFrame, ruc_cliente: str) -> DataFrame:
    """Columnas derivadas pensadas para el EDA y el modelo de la sección 4."""
    ff = F.col("fecha_formalizacion")
    pref = F.substring("ruc_proveedor", 1, 2)
    return df.select(
        "*",
        F.year(ff).alias("anio"),
        F.month(ff).alias("mes"),
        F.to_date(ff).alias("fecha_formalizacion_dia"),
        F.dayofweek(ff).alias("dia_semana"),  # 1=domingo ... 7=sábado
        F.dayofmonth(ff).alias("dia_mes"),
        # Marcador del cliente: el filtro se aplica en consumo, no aquí (el
        # modelo aprende de TODAS las entidades; ver sección 1.6).
        (F.col("ruc_entidad") == F.lit(ruc_cliente)).alias("es_mpt"),
        F.when(pref.isin(RUC_PERSONA_NATURAL), "NATURAL").when(pref == "20", "JURIDICA")
         .otherwise("OTRO").alias("tipo_persona_proveedor"),
        # Tiempo de ciclo hasta el último estado registrado.
        F.datediff("fecha_ultimo_estado", ff).alias("dias_formalizacion_a_ultimo_estado"),
        # Ventana de observación: el estado publicado es el estado A LA FECHA DE
        # CORTE del archivo (FECHA_PROCESO), no el estado final. Esta columna
        # dice cuántos días tuvo la orden para "volverse" adversa.
        F.datediff("fecha_proceso", ff).alias("dias_observacion_hasta_corte"),
        F.col("estado_orden").isin(ESTADOS_ADVERSOS).alias("flag_estado_adverso"),
        (F.col("estado_orden") == "PAGADA").alias("flag_pagada"),
        F.col("url_documento_estado").isNotNull().alias("flag_documento_estado"),
        (F.col("igv") == 0).alias("flag_igv_cero"),
        F.round(F.col("igv") / F.col("sub_total"), 4).cast("double").alias("tasa_igv"),
        # Los montos están muy sesgados (de S/ 0.05 a S/ 44.6 millones); el
        # logaritmo es la escala natural para comparar y detectar atípicos.
        F.log10(F.col("total").cast("double")).alias("log10_total"),
        F.col("total").cast("double").alias("total_double"),
    )


COLUMNAS_SALIDA = [
    "orden_electronica", "fecha_formalizacion", "fecha_formalizacion_dia", "fecha_ultimo_estado", "fecha_proceso",
    "ruc_entidad", "entidad", "es_mpt", "ruc_proveedor", "proveedor", "tipo_persona_proveedor",
    "tipo_procedimiento", "acuerdo_marco", "codigo_acuerdo_marco", "acuerdo_marco_original",
    "estado_orden", "descripcion_estado", "flag_estado_adverso", "flag_pagada", "flag_documento_estado",
    "sub_total", "igv", "total", "total_double", "log10_total", "tasa_igv", "flag_igv_cero",
    "dias_formalizacion_a_ultimo_estado", "dias_observacion_hasta_corte", "dia_semana", "dia_mes",
    "url_orden_generada", "url_orden_digitalizada", "url_documento_estado",
    "alertas_calidad", "archivo_origen", "periodo_archivo", "anio", "mes",
]


def verificar_esquema_versionado(df: DataFrame, crear_si_falta: bool) -> None:
    """El esquema de salida es un contrato: si cambia, se falla y se versiona."""
    actual = [{"nombre": f.name, "tipo": f.dataType.simpleString(), "nulable": f.nullable} for f in df.schema.fields]
    if not SCHEMA_FILE.exists():
        if not crear_si_falta:
            raise FileNotFoundError(SCHEMA_FILE)
        SCHEMA_FILE.parent.mkdir(parents=True, exist_ok=True)
        SCHEMA_FILE.write_text(json.dumps({"dataset": FUENTE, "version": SCHEMA_VERSION,
                                           "particiones": ["anio", "mes"], "clave": ["orden_electronica"],
                                           "columnas": actual}, ensure_ascii=False, indent=1), encoding="utf-8")
        return
    esperado = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))["columnas"]
    if [(c["nombre"], c["tipo"]) for c in esperado] != [(c["nombre"], c["tipo"]) for c in actual]:
        raise ValueError(f"El esquema de salida ya no coincide con {SCHEMA_FILE.name}: "
                         "crear una nueva versión (v2) en lugar de sobrescribir el contrato")


# ---------------------------------------------------------------- principal
def ejecutar(spark: SparkSession, cfg: dict, raw_root: Path, processed_out: str, cuarentena_out: str,
             reportes_dir: Path | None, log) -> dict:
    t0 = time.perf_counter()
    manifest = read_csv_dicts(raw_root / "_manifiesto.csv")
    if not manifest:
        raise FileNotFoundError(f"No hay manifiesto en {raw_root}: ejecutar primero la ingesta")
    verificar_cabeceras(manifest, raw_root)
    log.info("Cabeceras verificadas en %d archivos (contrato de 19 columnas OK)", len(manifest))

    crudo = leer_crudo(spark, raw_root, manifest)
    reglas_cfg = cargar_reglas(str(REGLAS_FILE))
    reglas = reglas_cfg["reglas"]

    df = normalizar(crudo)
    # R03: unicidad de la orden. Si una orden se repitiera en dos cortes, se
    # conserva la del corte más reciente (estado más actualizado).
    w = Window.partitionBy("orden_electronica").orderBy(F.col("fecha_proceso").desc_nulls_last(),
                                                         F.col("archivo_origen").desc())
    df = df.withColumn("_rn_orden", F.when(F.col("orden_electronica").isNull(), 1).otherwise(F.row_number().over(w)))
    df = derivar(df, cfg["entidad_cliente"]["ruc"])
    df = aplicar_reglas(df, reglas).cache()

    total = df.count()
    res_reglas = resumen_reglas(df, reglas, total)
    comp_antes = completitud(df, reglas_cfg["campos_criticos"], "entrada (tras normalizar)")

    validos = df.where(F.size("motivos_rechazo") == 0).select(*COLUMNAS_SALIDA)
    cuarentena = (df.where(F.size("motivos_rechazo") > 0)
                    .select(*[n for _, n in COLUMNAS], "_corrupt_record", "archivo_origen", "periodo_archivo",
                            "motivos_rechazo", F.lit(sello_tiempo()).alias("fecha_rechazo_utc")))

    verificar_esquema_versionado(validos, crear_si_falta=reportes_dir is not None)
    # 1 archivo por partición anio/mes (57 particiones de ~0.5-1.5 MB): suficiente
    # para leer en paralelo sin generar miles de archivos pequeños.
    write_parquet_atomic(validos, processed_out, partition_by=["anio", "mes"], num_files=len(manifest))
    write_parquet_atomic(cuarentena, cuarentena_out, num_files=1)
    # Copia del contrato de esquema junto a los datos: quien lea el Parquet
    # (Hugo, el tablero, DuckDB) sabe qué versión está consumiendo.
    if SCHEMA_FILE.exists():
        write_text_atomic(spark, processed_out.rstrip("/") + "/_esquema.json", SCHEMA_FILE.read_text(encoding="utf-8"))

    n_val = spark.read.parquet(processed_out).count()
    n_cua = spark.read.parquet(cuarentena_out).count()
    comp_despues = completitud(spark.read.parquet(processed_out), reglas_cfg["campos_criticos"], "salida (processed)")
    df.unpersist()

    metricas = {"ejecucion_utc": sello_tiempo(), "archivos": len(manifest), "registros_leidos": total,
                "registros_validos": n_val, "registros_cuarentena": n_cua,
                "cuadra": total == n_val + n_cua, "segundos": round(time.perf_counter() - t0, 2),
                "processed": processed_out, "cuarentena": cuarentena_out}
    log.info("Leídos %d | válidos %d | cuarentena %d | cuadra=%s | %.1f s", total, n_val, n_cua,
             metricas["cuadra"], metricas["segundos"])
    for r in res_reglas:
        log.info("  %-32s %-11s incumplen %7d (%.4f %%)", r["regla"], r["severidad"], r["incumplen"], r["pct_incumplen"])

    if reportes_dir is not None:
        reportes_dir.mkdir(parents=True, exist_ok=True)
        write_csv_atomic(reportes_dir / "calidad_reglas_fuente1.csv", res_reglas, list(res_reglas[0].keys()))
        write_csv_atomic(reportes_dir / "calidad_completitud_fuente1.csv", comp_antes + comp_despues,
                         list(comp_antes[0].keys()))
        (reportes_dir / "metricas_limpieza_fuente1.json").write_text(json.dumps(metricas, indent=1), encoding="utf-8")
    return metricas


def main() -> dict:
    cfg = load_config()
    log = get_logger("limpieza.f1", REPO_ROOT / "logs" / "limpieza_fuente1.log")
    spark = get_spark(cfg, "limpieza-f1")
    try:
        return ejecutar(
            spark, cfg,
            raw_root=Path(zone_path(cfg, "raw", FUENTE)),
            processed_out=zone_path(cfg, "processed", FUENTE, SCHEMA_VERSION),
            cuarentena_out=zone_path(cfg, "cuarentena", FUENTE),
            reportes_dir=REPO_ROOT / "docs" / "evidencias",
            log=log,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
