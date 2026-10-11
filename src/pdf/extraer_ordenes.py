"""Paso 8 (extracción) — Detalle por ítem desde el PDF de cada orden de la MPT.

Entrada : raw/ordenes_pdf/generada/anio=/mes=/<OCAM>.pdf (texto nativo, sin OCR)
Salida  : processed/items_orden_mpt     (una fila por ítem)
          processed/ordenes_pdf_cabecera (una fila por orden: campos no personales)

Cómo se lee el PDF (verificado sobre los archivos reales):
* El PDF trae TODAS las versiones de la orden (OCAM-...-0, -1, -2...). La
  última versión es la vigente y es la única que muestra "Precio Unitario".
* pdfplumber no reconoce tabla (no hay líneas de celda), así que cada ítem se
  reconoce por su forma: empieza con el número de ítem y termina con
  `<código único> <cantidad> <precio unitario sin IGV> <IGV> <importe>`.
  La descripción puede continuar en las líneas siguientes.
* Control cruzado: la suma de importes debe igualar el "IMPORTE TOTAL" del PDF
  y el TOTAL del CSV de la Fuente 1 (se verifica en la integración).

Datos personales: el PDF incluye nombre y DNI de servidores, correos y
teléfonos. NO se extraen; solo campos de la contratación y de los ítems.

Paralelismo: cada PDF es independiente, así que Spark reparte la lista de
archivos entre los núcleos (o nodos) y cada tarea procesa los suyos.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType, StructField, StructType)

from src.common.io_utils import get_logger, read_csv_dicts, write_csv_atomic
from src.common.spark_io import write_parquet_atomic
from src.config import REPO_ROOT, load_config, zone_path
from src.spark_session import get_spark

RE_VERSION = re.compile(r"(OCAM-\d{4}-\d+-\d+)-(\d+)")
NUM = r"\d+(?:,\d{3})*(?:\.\d+)?"  # con o sin separador de miles: "3000" y "3,000"
# Ítem con precio unitario (última versión): n desc ... codigo cant pu igv importe
RE_ITEM_PU = re.compile(rf"^(\d{{1,3}}) (.+?) (\S+) ({NUM}) ({NUM}) ({NUM}) ({NUM})$")
# Ítem sin precio unitario (versiones antiguas de una sola página): n desc ... codigo cant importe
RE_ITEM_SIMPLE = re.compile(rf"^(\d{{1,3}}) (.+?) (\S+) ({NUM}) ({NUM})$")
RE_TOTAL = re.compile(rf"IMPORTE TOTAL \(PEN\)\s*({NUM})")
RE_ENTREGA = re.compile(r"Número de entrega\s*:\s*(\d+) de (\d+)")
CAMPOS_CABECERA = {
    "fecha_formalizacion_pdf": r"Fecha de formalización\s*:\s*(\d{2}/\d{2}/\d{4})",
    "procedimiento_compra": r"Procedimiento de Compra\s*:\s*([A-Za-zÁÉÍÓÚáéíóú ]+?)(?:\s{2,}|\n|$)",
    "tipo_contratacion": r"Tipo de Contratación\s*:\s*(\w+)",
    "tipo_entrega": r"Tipo de Entrega\s*:\s*([A-Za-zÁÉÍÓÚáéíóú ]+?)(?:\n|$)",
    "total_entregas": r"Total de entregas\s*:\s*(\d+)",
    "emergencia": r"Emergencia\s*:\s*(SI|NO)",
    "expediente_siaf": r"Expediente SIAF\s*:\s*(\d+)",
    "plazo_desde": r"Plazo de entrega\s*:\s*Del (\d{2}/\d{2}/\d{4})",
    "plazo_hasta": r"Plazo de entrega\s*:\s*Del \d{2}/\d{2}/\d{4} al (\d{2}/\d{2}/\d{4})",
    "estrategia_compra": r"Estrategia de compra\s*:\s*([A-Za-zÁÉÍÓÚáéíóú ]+?)(?:\n|$)",
}

SCHEMA_ITEMS = StructType([
    StructField("orden_electronica", StringType()), StructField("version_orden", IntegerType()),
    StructField("nro_item", IntegerType()), StructField("producto", StringType()),
    StructField("descripcion", StringType()), StructField("codigo_unico", StringType()),
    StructField("cantidad", DoubleType()), StructField("precio_unitario_sin_igv", DoubleType()),
    StructField("igv_item", DoubleType()), StructField("importe_item", DoubleType()),
    StructField("tiene_precio_unitario", StringType()), StructField("item_cuadra", StringType()),
])
SCHEMA_CAB = StructType([
    StructField("orden_electronica", StringType()), StructField("version_orden", IntegerType()),
    StructField("paginas", IntegerType()), StructField("n_items", IntegerType()),
    StructField("n_entregas_pdf", IntegerType()),
    StructField("importe_total_pdf", DoubleType()), StructField("suma_items", DoubleType()),
    *[StructField(k, StringType()) for k in CAMPOS_CABECERA],
    StructField("estado_extraccion", StringType()), StructField("archivo", StringType()),
])


def _num(s: str) -> float:
    return float(s.replace(",", ""))


def parsear_pdf(ruta: str) -> tuple[list[dict], dict]:
    """Devuelve (ítems, cabecera) de un PDF. Función pura: fácil de probar."""
    import pdfplumber  # import local: se ejecuta en los procesos de Spark

    with pdfplumber.open(ruta) as pdf:
        paginas = [(p.extract_text() or "") for p in pdf.pages]
    # Agrupar páginas por versión (OCAM-...-0, -1, -2...).
    por_version: dict[int, list[str]] = {}
    base, ultima = None, -1
    for txt in paginas:
        m = RE_VERSION.search(txt)
        v = int(m.group(2)) if m else ultima
        if m:
            base = m.group(1)
        por_version.setdefault(v, []).append(txt)
        ultima = v
    orden = f"{base}-0" if base else Path(ruta).stem  # clave = código de la Fuente 1 (sufijo -0)
    textos = {v: "\n".join(t) for v, t in por_version.items()}

    # Dos significados distintos de las versiones, verificados en los PDF reales:
    #  * Entrega única: -1 es la orden aceptada (con precio unitario) que
    #    reemplaza a -0. Se usa la ÚLTIMA versión.
    #  * Entregas parciales: cada versión -1..-N es UNA ENTREGA ("Número de
    #    entrega : i de N") con parte de las cantidades. La orden completa es la
    #    SUMA de las entregas (usar solo la última subestimaba el total; error
    #    detectado al cruzar con el TOTAL del CSV en 9 órdenes).
    entregas = {v: t for v, t in textos.items()
                if (m := RE_ENTREGA.search(t)) and int(m.group(2)) > 1}
    if len(entregas) > 1:
        version, n_entregas = max(entregas), len(entregas)
        items = _sumar_entregas([_items_de_texto(t, orden, v) for v, t in sorted(entregas.items())])
        total = round(sum(_num(m.group(1)) for t in entregas.values() if (m := RE_TOTAL.search(t))), 2)
        texto = textos[version]
    else:
        version, n_entregas = max(textos), 1
        texto = textos[version]
        items = _items_de_texto(texto, orden, version)
        tot = RE_TOTAL.search(texto)
        total = _num(tot.group(1)) if tot else None

    cab = {"orden_electronica": orden, "version_orden": version, "paginas": len(paginas), "n_items": len(items),
           "n_entregas_pdf": n_entregas, "importe_total_pdf": total,
           "suma_items": round(sum(i["importe_item"] for i in items), 2) if items else None,
           "archivo": Path(ruta).name}
    for k, pat in CAMPOS_CABECERA.items():
        mm = re.search(pat, texto)
        cab[k] = mm.group(1).strip() if mm else None
    if not items:
        cab["estado_extraccion"] = "SIN_ITEMS"
    elif cab["importe_total_pdf"] is None or abs(cab["suma_items"] - cab["importe_total_pdf"]) > 0.05:
        cab["estado_extraccion"] = "ITEMS_NO_CUADRAN_CON_TOTAL"
    else:
        cab["estado_extraccion"] = "OK"
    return items, cab


def _items_de_texto(texto: str, orden: str, version: int) -> list[dict]:
    items, actual = [], None
    for linea in texto.split("\n"):
        linea = linea.strip()
        if linea.startswith("IMPORTE TOTAL") or linea.startswith("SISCATALOGO"):
            actual = None
            continue
        m = RE_ITEM_PU.match(linea)
        simple = None if m else RE_ITEM_SIMPLE.match(linea)
        if m or simple:
            g = (m or simple).groups()
            n, desc, cod = int(g[0]), g[1], g[2]
            if m:
                cant, pu, igv, imp = _num(g[3]), _num(g[4]), _num(g[5]), _num(g[6])
            else:
                cant, imp = _num(g[3]), _num(g[4])
                pu, igv = None, None
            # Si el número de ítem no es el siguiente esperado, la línea no es un ítem
            # (p. ej. una línea de descripción que empieza con un número).
            if n == len(items) + 1:
                actual = {"orden_electronica": orden, "version_orden": version, "nro_item": n,
                          "producto": desc.split(" : ")[0].strip()[:120], "descripcion": desc,
                          "codigo_unico": cod, "cantidad": cant, "precio_unitario_sin_igv": pu,
                          "igv_item": igv, "importe_item": imp}
                items.append(actual)
                continue
        if actual is not None and linea and not linea.startswith(("Nro Ficha", "de Producto", "Producto Total")):
            actual["descripcion"] = (actual["descripcion"] + " " + linea)[:2000]
    for it in items:
        _marcar(it)
    return items


def _marcar(it: dict) -> None:
    pu, igv, imp, cant = it["precio_unitario_sin_igv"], it["igv_item"], it["importe_item"], it["cantidad"]
    it["tiene_precio_unitario"] = str(pu is not None)
    it["item_cuadra"] = str(pu is not None and abs(cant * pu + igv - imp) <= max(0.05, 0.001 * imp))


def _sumar_entregas(listas: list[list[dict]]) -> list[dict]:
    """Une los ítems de varias entregas: suma cantidades, IGV e importes por
    número de ítem; el precio unitario es el mismo en todas las entregas."""
    acum: dict[int, dict] = {}
    for items in listas:
        for it in items:
            a = acum.get(it["nro_item"])
            if a is None:
                acum[it["nro_item"]] = dict(it)
                continue
            a["cantidad"] += it["cantidad"]
            a["importe_item"] = round(a["importe_item"] + it["importe_item"], 2)
            if a["igv_item"] is not None and it["igv_item"] is not None:
                a["igv_item"] = round(a["igv_item"] + it["igv_item"], 2)
            a["version_orden"] = max(a["version_orden"], it["version_orden"])
    out = [acum[k] for k in sorted(acum)]
    for it in out:
        _marcar(it)
    return out


def _procesar(ruta: str):
    try:
        items, cab = parsear_pdf(ruta)
    except Exception as e:  # PDF dañado: se registra, no se oculta
        cab = {k: None for k in SCHEMA_CAB.names}
        cab.update({"orden_electronica": Path(ruta).stem, "archivo": Path(ruta).name,
                    "estado_extraccion": f"ERROR:{type(e).__name__}"})
        items = []
    return [("item", i) for i in items] + [("cab", cab)]


def main() -> dict:
    cfg = load_config()
    log = get_logger("pdf.ordenes", REPO_ROOT / "logs" / "extraccion_ordenes_pdf.log")
    raw_root = Path(zone_path(cfg, "raw", "ordenes_pdf"))
    manifest = read_csv_dicts(raw_root / "_manifiesto.csv")
    rutas = [str(raw_root / m["ruta_relativa"]) for m in manifest
             if m["tipo_documento"] == "generada" and m["es_pdf"] == "True"]
    spark = get_spark(cfg, "extraccion-ordenes-pdf")
    t0 = time.perf_counter()
    try:
        # Una tarea por cada ~25 PDF: reparte la carga entre todos los núcleos.
        rdd = spark.sparkContext.parallelize(rutas, max(1, len(rutas) // 25)).flatMap(_procesar).cache()
        # Cada fila se arma como tupla EN EL ORDEN DEL ESQUEMA: Spark asigna por
        # posición, no por nombre (un Row(**dict) desalineaba las columnas).
        items_n, cab_n = SCHEMA_ITEMS.names, SCHEMA_CAB.names
        items = spark.createDataFrame(rdd.filter(lambda x: x[0] == "item")
                                         .map(lambda x: tuple(x[1].get(c) for c in items_n)), SCHEMA_ITEMS)
        cab = spark.createDataFrame(rdd.filter(lambda x: x[0] == "cab")
                                       .map(lambda x: tuple(x[1].get(c) for c in cab_n)), SCHEMA_CAB)
        a_fecha = lambda c: F.to_date(F.col(c), "dd/MM/yyyy")  # noqa: E731
        cab = (cab.withColumn("fecha_formalizacion_pdf", a_fecha("fecha_formalizacion_pdf"))
                  .withColumn("plazo_desde", a_fecha("plazo_desde")).withColumn("plazo_hasta", a_fecha("plazo_hasta"))
                  .withColumn("dias_plazo_entrega", F.datediff("plazo_hasta", "plazo_desde")))
        items = items.withColumn("item_cuadra", F.col("item_cuadra") == "True") \
                     .withColumn("tiene_precio_unitario", F.col("tiene_precio_unitario") == "True")
        write_parquet_atomic(items, zone_path(cfg, "processed", "items_orden_mpt"), num_files=1)
        write_parquet_atomic(cab, zone_path(cfg, "processed", "ordenes_pdf_cabecera"), num_files=1)
        rdd.unpersist()

        it = spark.read.parquet(zone_path(cfg, "processed", "items_orden_mpt"))
        cb = spark.read.parquet(zone_path(cfg, "processed", "ordenes_pdf_cabecera"))
        estados = {r["estado_extraccion"]: r["count"] for r in cb.groupBy("estado_extraccion").count().collect()}
        res = {"pdf_procesados": cb.count(), "items": it.count(), "estados_extraccion": estados,
               "items_con_precio_unitario": it.where("tiene_precio_unitario").count(),
               "items_que_cuadran": it.where("item_cuadra").count(),
               "segundos": round(time.perf_counter() - t0, 2)}
        log.info("RESUMEN extracción órdenes PDF: %s", res)
        write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "extraccion_ordenes_pdf.csv",
                         [{"medida": k, "valor": v} for k, v in res.items()], ["medida", "valor"])
        return res
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
