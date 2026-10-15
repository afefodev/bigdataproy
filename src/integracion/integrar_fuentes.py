"""Paso 9 — Integración de la Fuente 1 (CSV) con la Fuente 2 (PDF) (sección 3.3).

Tres cruces, del más fuerte al más débil, y para cada uno se REPORTA cuántas
órdenes cruzan y cuántas no (docs/evidencias/integracion_resumen.csv):

1. Órdenes MPT  ⟷  PDF de la orden            clave exacta: orden_electronica
   Es la unión principal: mismo identificador en ambas fuentes. Además permite
   un control cruzado de veracidad: el TOTAL del CSV debe ser igual al
   IMPORTE TOTAL impreso en el PDF.
2. Órdenes MPT  ⟷  resoluciones (ventana)     |fecha_formalización − fecha_resolución| ≤ 30 días
   Propuesta del documento v4. Se implementa y se mide, pero la revisión de la
   muestra real muestra que es una relación débil (ver informe).
3. Órdenes MPT  ⟷  resoluciones (temático)    ventana + la resolución menciona
   bienes de la misma familia (acuerdo marco) que la orden.

Registros sin correspondencia: se conservan (left join) con indicadores
`tiene_pdf`, `n_resoluciones_ventana`, etc. Ninguna orden se descarta por no cruzar.
"""
from __future__ import annotations

import time
import unicodedata
from functools import reduce

import yaml
from pyspark.sql import functions as F

from src.common.io_utils import get_logger, write_csv_atomic
from src.common.spark_io import write_parquet_atomic
from src.config import REPO_ROOT, load_config, zone_path
from src.spark_session import get_spark


def _sin_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def main() -> dict:
    cfg = load_config()
    icfg = yaml.safe_load(open(REPO_ROOT / "config" / "integracion.yaml", encoding="utf-8"))
    log = get_logger("integracion", REPO_ROOT / "logs" / "integracion.log")
    spark = get_spark(cfg, "integracion")
    t0 = time.perf_counter()
    try:
        ordenes = (spark.read.parquet(zone_path(cfg, "processed", "ordenes_compra", "v1"))
                   .where("es_mpt")
                   .select("orden_electronica", "fecha_formalizacion", "fecha_formalizacion_dia", "anio", "mes",
                           "acuerdo_marco", "tipo_procedimiento", "estado_orden", "flag_estado_adverso",
                           "ruc_proveedor", "tipo_persona_proveedor", "total", "total_double", "log10_total"))
        n_ord = ordenes.count()

        # ---------------------------------------------------- 1. clave exacta
        cab = spark.read.parquet(zone_path(cfg, "processed", "ordenes_pdf_cabecera"))
        items = spark.read.parquet(zone_path(cfg, "processed", "items_orden_mpt"))
        agg_items = items.groupBy("orden_electronica").agg(
            F.count("*").alias("n_items"),
            F.sum("cantidad").alias("unidades_totales"),
            F.countDistinct("producto").alias("productos_distintos"),
            F.max("importe_item").alias("importe_item_max"),
            F.first("producto", ignorenulls=True).alias("producto_principal"),
        )
        pdf = (cab.select("orden_electronica", "version_orden", "importe_total_pdf", "estado_extraccion",
                          "fecha_formalizacion_pdf", "tipo_contratacion", "tipo_entrega", "total_entregas",
                          "emergencia", "expediente_siaf", "dias_plazo_entrega")
                  .join(agg_items, "orden_electronica", "left")
                  .withColumn("tiene_pdf", F.lit(True)))
        o1 = ordenes.join(pdf, "orden_electronica", "left").fillna({"tiene_pdf": False})
        o1 = (o1.withColumn("total_cuadra_csv_pdf",
                            F.when(F.col("tiene_pdf"), F.abs(F.col("total_double") - F.col("importe_total_pdf")) <= 0.05))
                .withColumn("fecha_cuadra_csv_pdf",
                            F.when(F.col("tiene_pdf"), F.col("fecha_formalizacion_dia") == F.col("fecha_formalizacion_pdf"))))

        # ------------------------------------------- 2 y 3. resoluciones
        res = spark.read.parquet(zone_path(cfg, "processed", "resoluciones_mpt"))
        texto_plano = F.translate(F.upper(F.col("texto")), "ÁÉÍÓÚÜ", "AEIOUU")
        # Una familia se considera mencionada si aparece CUALQUIERA de sus palabras clave.
        fams = [F.when(reduce(lambda a, b: a | b, [texto_plano.contains(_sin_tildes(k)) for k in kws]), F.lit(fam))
                for fam, kws in icfg["familias"].items()]
        res = (res.withColumn("fecha_resolucion", F.coalesce("fecha_emision", "fecha_publicacion"))
                  .withColumn("familias_mencionadas", F.filter(F.array(*fams), lambda x: x.isNotNull())))
        res_min = res.select("numero", "fecha_resolucion", "tema", "familias_mencionadas")
        v = icfg["ventana_dias"]
        # La tabla de resoluciones es diminuta (5 filas): broadcast evita barajar las órdenes.
        cruce = (o1.select("orden_electronica", "fecha_formalizacion_dia", "acuerdo_marco")
                   .join(F.broadcast(res_min),
                         F.abs(F.datediff("fecha_formalizacion_dia", "fecha_resolucion")) <= v, "inner")
                   .withColumn("misma_familia", F.array_contains("familias_mencionadas", F.col("acuerdo_marco"))))
        por_orden = cruce.groupBy("orden_electronica").agg(
            F.count("*").alias("n_resoluciones_ventana"),
            F.sum(F.col("misma_familia").cast("int")).alias("n_resoluciones_misma_familia"),
            F.concat_ws(";", F.collect_set("numero")).alias("resoluciones_ventana"),
        )
        integ = (o1.join(por_orden, "orden_electronica", "left")
                   .fillna({"n_resoluciones_ventana": 0, "n_resoluciones_misma_familia": 0}))
        write_parquet_atomic(integ, zone_path(cfg, "processed", "ordenes_mpt_integradas"), num_files=1)
        integ = spark.read.parquet(zone_path(cfg, "processed", "ordenes_mpt_integradas"))

        # Resumen de resoluciones para consumo: SIN el texto completo.
        res_res = (res.join(cruce.groupBy("numero").agg(
                                F.count("*").alias("ordenes_en_ventana"),
                                F.sum(F.col("misma_familia").cast("int")).alias("ordenes_misma_familia")),
                            "numero", "left")
                      .fillna({"ordenes_en_ventana": 0, "ordenes_misma_familia": 0})
                      .select("numero", "tipo_resolucion", "unidad_emisora", "fecha_publicacion", "fecha_emision",
                              "fecha_resolucion", "tema", "accion_principal", "expedientes_citados",
                              F.concat_ws(";", "familias_mencionadas").alias("familias_mencionadas"),
                              "paginas", "confianza_ocr_cuerpo", "ordenes_en_ventana", "ordenes_misma_familia"))
        write_parquet_atomic(res_res, zone_path(cfg, "processed", "resoluciones_mpt_resumen"), num_files=1)

        # ------------------------------------------------------- métricas
        c = integ.agg(
            F.sum(F.col("tiene_pdf").cast("int")).alias("con_pdf"),
            F.sum((F.col("estado_extraccion") == "OK").cast("int")).alias("pdf_ok"),
            F.sum(F.col("total_cuadra_csv_pdf").cast("int")).alias("total_cuadra"),
            F.sum(F.col("fecha_cuadra_csv_pdf").cast("int")).alias("fecha_cuadra"),
            F.sum((F.col("n_resoluciones_ventana") > 0).cast("int")).alias("con_resolucion_ventana"),
            F.sum((F.col("n_resoluciones_misma_familia") > 0).cast("int")).alias("con_resolucion_misma_familia"),
        ).first().asDict()
        exp_res = {e for r in res.select("expedientes_citados").collect() if r[0] for e in r[0].split(";")}
        exp_ord = {r[0] for r in integ.select("expediente_siaf").where("expediente_siaf is not null").distinct().collect()}
        coincid_exp = len({e.split("-")[0] for e in exp_res} & exp_ord)
        ocam_citadas = res.where("ocam_citadas is not null").count()
        filas = [
            {"cruce": "1. CSV ⟷ PDF de la orden (orden_electronica)", "ordenes_mpt": n_ord, "cruzan": c["con_pdf"],
             "no_cruzan": n_ord - c["con_pdf"],
             "detalle": f"PDF extraído OK: {c['pdf_ok']}; TOTAL CSV = PDF: {c['total_cuadra']}; fecha CSV = PDF: {c['fecha_cuadra']}"},
            {"cruce": f"2. Órdenes ⟷ resoluciones (±{v} días)", "ordenes_mpt": n_ord, "cruzan": c["con_resolucion_ventana"],
             "no_cruzan": n_ord - c["con_resolucion_ventana"],
             "detalle": f"{res.count()} resoluciones de abastecimiento 2022-2026 en gob.pe; todas sobre bajas de bienes"},
            {"cruce": f"3. Órdenes ⟷ resoluciones (±{v} días y misma familia)", "ordenes_mpt": n_ord,
             "cruzan": c["con_resolucion_misma_familia"], "no_cruzan": n_ord - c["con_resolucion_misma_familia"],
             "detalle": "familia = acuerdo marco de la orden mencionado en el texto OCR de la resolución"},
            {"cruce": "Clave alternativa: expediente", "ordenes_mpt": n_ord, "cruzan": coincid_exp, "no_cruzan": n_ord - coincid_exp,
             "detalle": f"expedientes citados en resoluciones (mesa de partes): {len(exp_res)}; expedientes SIAF en PDF: {len(exp_ord)}; numeraciones distintas"},
            {"cruce": "Clave alternativa: código OCAM citado en la resolución", "ordenes_mpt": n_ord, "cruzan": 0,
             "no_cruzan": n_ord, "detalle": f"resoluciones que citan algún OCAM: {ocam_citadas}"},
        ]
        write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "integracion_resumen.csv", filas,
                         ["cruce", "ordenes_mpt", "cruzan", "no_cruzan", "detalle"])
        res_out = [r.asDict() for r in spark.read.parquet(zone_path(cfg, "processed", "resoluciones_mpt_resumen"))
                   .select("numero", "fecha_resolucion", "tema", "familias_mencionadas", "ordenes_en_ventana",
                           "ordenes_misma_familia").collect()]
        write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "integracion_resoluciones.csv", res_out, list(res_out[0].keys()))
        out = {"ordenes_mpt": n_ord, **c, "segundos": round(time.perf_counter() - t0, 2)}
        log.info("RESUMEN integración: %s", out)
        for f in filas:
            log.info("  %s -> cruzan %s / no cruzan %s (%s)", f["cruce"], f["cruzan"], f["no_cruzan"], f["detalle"])
        return out
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
