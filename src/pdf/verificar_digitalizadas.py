"""Paso 8 — ¿Qué aportan los PDF de ORDEN_DIGITALIZADA? (muestra de verificación)

Las órdenes digitalizadas son escaneos (imagen) que sube la entidad. Para decidir
si vale la pena procesarlas en masa, se aplica OCR a la primera página de la
muestra descargada y se comprueba si el texto reconocido contiene el código de
la orden y el monto total del CSV, es decir, si es la misma orden y si su
lectura es confiable.

Salida: docs/evidencias/verificacion_digitalizadas.csv (sin texto: solo
indicadores, porque el escaneo incluye firmas y nombres).
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pyarrow.dataset as ds

from src.common.io_utils import get_logger, read_csv_dicts, write_csv_atomic
from src.config import REPO_ROOT, load_config, zone_path


def main() -> dict:
    os.environ.setdefault("OMP_THREAD_LIMIT", "2")
    import pdfplumber
    import pytesseract

    cfg = load_config()
    log = get_logger("pdf.digitalizadas", REPO_ROOT / "logs" / "verificacion_digitalizadas.log")
    raw_root = Path(zone_path(cfg, "raw", "ordenes_pdf"))
    muestra = [m for m in read_csv_dicts(raw_root / "_manifiesto.csv")
               if m["tipo_documento"] == "digitalizada" and m["es_pdf"] == "True"]
    proc = ds.dataset(zone_path(cfg, "processed", "ordenes_compra", "v1"), format="parquet", partitioning="hive")
    totales = {r["orden_electronica"]: r["total_double"] for r in
               proc.to_table(columns=["orden_electronica", "total_double"], filter=ds.field("es_mpt") == True).to_pylist()}  # noqa: E712
    filas = []
    for m in muestra:
        with pdfplumber.open(raw_root / m["ruta_relativa"]) as pdf:
            n_pag = len(pdf.pages)
            capa_texto = len((pdf.pages[0].extract_text() or "").strip())
            img = pdf.pages[0].to_image(resolution=200).original
        datos = pytesseract.image_to_data(img, lang="spa", output_type=pytesseract.Output.DICT)
        confs = [float(c) for c, w in zip(datos["conf"], datos["text"]) if w.strip() and float(c) >= 0]
        texto = " ".join(w for w in datos["text"] if w.strip()).upper()
        base = re.sub(r"-\d+$", "", m["orden_electronica"])  # OCAM-AAAA-UE-N
        total = totales.get(m["orden_electronica"])
        total_txt = f"{total:,.2f}" if total is not None else "?"
        filas.append({
            "orden_electronica": m["orden_electronica"], "paginas": n_pag, "tiene_capa_texto": capa_texto > 50,
            "confianza_ocr_pag1": round(sum(confs) / len(confs), 1) if confs else None,
            "ocr_contiene_codigo_orden": base.replace(" ", "") in texto.replace(" ", ""),
            "ocr_contiene_total_csv": total_txt in texto,
        })
        log.info("%s", filas[-1])
    res = {"muestra": len(filas),
           "sin_capa_de_texto": sum(not f["tiene_capa_texto"] for f in filas),
           "codigo_reconocido": sum(f["ocr_contiene_codigo_orden"] for f in filas),
           "total_reconocido": sum(f["ocr_contiene_total_csv"] for f in filas)}
    log.info("RESUMEN digitalizadas: %s", res)
    write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "verificacion_digitalizadas.csv", filas, list(filas[0].keys()))
    return res


if __name__ == "__main__":
    main()
