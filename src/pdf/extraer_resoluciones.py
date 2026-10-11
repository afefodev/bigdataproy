"""Paso 8 (extracción) — Texto y campos estructurados de las resoluciones de la MPT.

Entrada : raw/resoluciones_mpt/anio=/mes=/*.pdf
Salida  : processed/resoluciones_mpt          (una fila por resolución)
          processed/resoluciones_mpt_paginas  (texto por página, para auditoría)

* OCR solo si hace falta: primero se intenta leer la capa de texto del PDF; si
  una página trae menos de 50 caracteres, se considera escaneada y se aplica
  Tesseract (idioma español). En la muestra real, las 5 resoluciones son
  escaneos (imagen), así que todas sus páginas pasan por OCR.
* La confianza media de Tesseract se guarda por página: es la medida de
  calidad de la extracción (sección 3.4).
* Paralelismo: Spark reparte las PÁGINAS (no los archivos) entre núcleos,
  porque una resolución puede tener 111 páginas y otra 9.
"""
from __future__ import annotations

import os
import re
import time
import unicodedata
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

from src.common.io_utils import get_logger, read_csv_dicts, write_csv_atomic
from src.common.spark_io import write_parquet_atomic
from src.config import REPO_ROOT, load_config, zone_path
from src.spark_session import get_spark

DPI = 200            # suficiente para texto mecanografiado; 300 duplica el tiempo sin mejora visible
MIN_CHARS_TEXTO = 50
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
         "setiembre": 9, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}

SCHEMA_PAG = StructType([
    StructField("id_norma", StringType()), StructField("pagina", IntegerType()), StructField("metodo", StringType()),
    StructField("confianza_ocr", DoubleType()), StructField("caracteres", IntegerType()), StructField("texto", StringType()),
])

# Esquema explícito: varias columnas (p. ej. ocam_citadas) pueden venir vacías en
# todas las resoluciones y Spark no podría inferir su tipo.
SCHEMA_RES = StructType([
    StructField("id_norma", StringType()), StructField("numero", StringType()),
    StructField("titulo_portal", StringType()), StructField("tipo_resolucion", StringType()),
    StructField("unidad_emisora", StringType()), StructField("menciona_abastecimiento", BooleanType()),
    StructField("fecha_publicacion", StringType()), StructField("fecha_emision", StringType()),
    StructField("accion_principal", StringType()), StructField("tema", StringType()),
    StructField("expedientes_citados", StringType()), StructField("ocam_citadas", StringType()),
    StructField("ordenes_compra_citadas", StringType()), StructField("paginas", IntegerType()),
    StructField("paginas_ocr", IntegerType()), StructField("confianza_ocr_media", DoubleType()),
    StructField("confianza_ocr_cuerpo", DoubleType()), StructField("confianza_ocr_anexos", DoubleType()),
    StructField("caracteres", IntegerType()), StructField("texto", StringType()), StructField("archivo", StringType()),
])


def leer_pagina(args: tuple[str, str, int]) -> Row:
    """Extrae una página: capa de texto si existe; si no, OCR."""
    os.environ.setdefault("OMP_THREAD_LIMIT", "1")  # 1 hilo por tarea: Spark ya paraleliza
    import pdfplumber
    import pytesseract

    id_norma, ruta, i = args
    with pdfplumber.open(ruta) as pdf:
        page = pdf.pages[i]
        texto = page.extract_text() or ""
        if len(texto.strip()) >= MIN_CHARS_TEXTO:
            return Row(id_norma=id_norma, pagina=i + 1, metodo="texto", confianza_ocr=None,
                       caracteres=len(texto), texto=texto)
        img = page.to_image(resolution=DPI).original
    datos = pytesseract.image_to_data(img, lang="spa", output_type=pytesseract.Output.DICT)
    confs = [float(c) for c, w in zip(datos["conf"], datos["text"]) if w.strip() and float(c) >= 0]
    texto = pytesseract.image_to_string(img, lang="spa")
    return Row(id_norma=id_norma, pagina=i + 1, metodo="ocr",
               confianza_ocr=round(sum(confs) / len(confs), 2) if confs else None,
               caracteres=len(texto), texto=texto)


def _sin_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def estructurar(meta: dict, paginas: list[dict]) -> dict:
    """Campos de la resolución a partir del texto (la página 1 tiene la cabecera)."""
    paginas = sorted(paginas, key=lambda p: p["pagina"])
    texto = "\n".join(p["texto"] for p in paginas)
    p1 = paginas[0]["texto"] if paginas else ""
    plano = _sin_tildes(texto.upper())

    # 'Trujillo, 21 de julio del 2025' o 'Trujillo, 02 de setiembre 2024' (sin "del").
    m_fecha = re.search(r"Trujillo,?\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(?:del?\s+)?(\d{4})", p1, re.I)
    fecha_emision = None
    if m_fecha and m_fecha.group(2).lower() in MESES:
        fecha_emision = f"{int(m_fecha.group(3)):04d}-{MESES[m_fecha.group(2).lower()]:02d}-{int(m_fecha.group(1)):02d}"
    tipo = "RESOLUCION JEFATURAL" if "RESOLUCION JEFATURAL" in _sin_tildes(p1.upper()) else (
        "RESOLUCION DE GERENCIA" if "RESOLUCION DE GERENCIA" in _sin_tildes(p1.upper()) else "OTRA")
    unidad = meta["numero"].split("-")[-1].upper() if meta.get("numero") else None
    # Expedientes administrativos citados (p. ej. 27789-2025-MPT): posible clave de unión.
    expedientes = sorted(set(re.findall(r"EXPEDIENTE\S*\s+N\S{0,3}\s*(\d{2,6}-\d{4}-MPT)", plano)))
    ocam = sorted(set(re.findall(r"OCAM-\d{4}-\d+-\d+", plano)))
    ordenes_compra = sorted(set(re.findall(r"ORDEN(?:ES)? DE COMPRA\s+N\S{0,3}\s*([\d-]+)", plano)))
    # Acción principal: verbo en mayúsculas después de "ARTÍCULO PRIMERO" (APROBAR, RECTIFICAR, ...).
    m_acc = re.search(r"ARTICULO\s+PRIMERO\S*\s*[.\-–:]*\s*([A-Z]{4,})", plano)
    asunto = None
    for clave in ("BAJA", "ALTA", "SANEAMIENTO", "RECTIFICA", "ADQUISICION", "CONTRATACION", "TRANSFERENCIA", "DONACION"):
        if clave in plano[:6000]:
            asunto = clave
            break
    ocr = [p["confianza_ocr"] for p in paginas if p["confianza_ocr"] is not None]
    # Cuerpo (págs. 1-3: vistos, considerandos, resuelve) vs. anexos (tablas de bienes).
    cuerpo = [p["confianza_ocr"] for p in paginas if p["confianza_ocr"] is not None and p["pagina"] <= 3]
    anexos = [p["confianza_ocr"] for p in paginas if p["confianza_ocr"] is not None and p["pagina"] > 3]
    return {
        "id_norma": meta["id_norma"], "numero": meta["numero"], "titulo_portal": meta["titulo"],
        "tipo_resolucion": tipo, "unidad_emisora": unidad,
        "menciona_abastecimiento": "ABASTECIMIENTO" in plano,
        "fecha_publicacion": meta["fecha_publicacion"], "fecha_emision": fecha_emision,
        "accion_principal": m_acc.group(1) if m_acc else None, "tema": asunto,
        "expedientes_citados": ";".join(expedientes) or None,
        "ocam_citadas": ";".join(ocam) or None, "ordenes_compra_citadas": ";".join(ordenes_compra) or None,
        "paginas": len(paginas), "paginas_ocr": sum(p["metodo"] == "ocr" for p in paginas),
        "confianza_ocr_media": round(sum(ocr) / len(ocr), 2) if ocr else None,
        "confianza_ocr_cuerpo": round(sum(cuerpo) / len(cuerpo), 2) if cuerpo else None,
        "confianza_ocr_anexos": round(sum(anexos) / len(anexos), 2) if anexos else None,
        "caracteres": len(texto), "texto": texto, "archivo": meta["nombre_archivo"],
    }


def main(forzar: bool = False) -> dict:
    cfg = load_config()
    log = get_logger("pdf.resoluciones", REPO_ROOT / "logs" / "extraccion_resoluciones.log")
    raw_root = Path(zone_path(cfg, "raw", "resoluciones_mpt"))
    manifest = read_csv_dicts(raw_root / "_manifiesto.csv")
    tareas = []
    for m in manifest:
        ruta = str(raw_root / f"anio={m['anio']}" / f"mes={m['mes']}" / m["nombre_archivo"])
        tareas += [(m["id_norma"], ruta, i) for i in range(int(m["paginas"]))]
    spark = get_spark(cfg, "extraccion-resoluciones")
    t0 = time.perf_counter()
    try:
        ruta_pags = zone_path(cfg, "processed", "resoluciones_mpt_paginas")
        try:
            previas = spark.read.parquet(ruta_pags)
            reusar = previas.count() == len(tareas) and not forzar
        except Exception:  # noqa: BLE001 — primera ejecución: aún no existe
            reusar = False
        if reusar:
            # Idempotencia: el OCR es la etapa cara (~10 min); si ya están todas
            # las páginas de los mismos PDF crudos, no se repite.
            log.info("Reutilizando %d páginas ya extraídas (usar --forzar para repetir el OCR)", len(tareas))
        else:
            pags = spark.createDataFrame(spark.sparkContext.parallelize(tareas, len(tareas)).map(leer_pagina), SCHEMA_PAG)
            write_parquet_atomic(pags, ruta_pags, num_files=1)
        pags = spark.read.parquet(ruta_pags)
        por_norma: dict[str, list[dict]] = {}
        for r in pags.collect():  # 206 filas: cabe holgadamente en el driver
            por_norma.setdefault(r["id_norma"], []).append(r.asDict())
        filas = [estructurar(m, por_norma.get(m["id_norma"], [])) for m in manifest]
        res_df = spark.createDataFrame([Row(**{c: f[c] for c in SCHEMA_RES.names}) for f in filas], SCHEMA_RES).withColumn("fecha_publicacion", F.to_date("fecha_publicacion")) \
                                             .withColumn("fecha_emision", F.to_date("fecha_emision"))
        write_parquet_atomic(res_df, zone_path(cfg, "processed", "resoluciones_mpt"), num_files=1)
        res = {"resoluciones": len(filas), "paginas": pags.count(),
               "paginas_ocr": pags.where("metodo = 'ocr'").count(),
               "confianza_ocr_media": round(pags.agg(F.avg("confianza_ocr")).first()[0] or 0, 2),
               "segundos": round(time.perf_counter() - t0, 2)}
        log.info("RESUMEN extracción resoluciones: %s", res)
        cols = ["numero", "tipo_resolucion", "unidad_emisora", "fecha_publicacion", "fecha_emision", "tema",
                "accion_principal", "menciona_abastecimiento", "expedientes_citados", "ocam_citadas",
                "ordenes_compra_citadas", "paginas", "paginas_ocr", "confianza_ocr_media", "confianza_ocr_cuerpo",
                "confianza_ocr_anexos", "caracteres"]
        write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "resoluciones_estructuradas.csv",
                         [{c: f[c] for c in cols} for f in filas], cols)
        return res
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    main(forzar="--forzar" in sys.argv)
