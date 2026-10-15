"""Paso 11 — Script maestro: ejecuta el pipeline de principio a fin con log.

    .venv/bin/python -m scripts.run_pipeline                 # pipeline completo
    .venv/bin/python -m scripts.run_pipeline --solo fuente1  # solo lo que necesita la sección 4
    .venv/bin/python -m scripts.run_pipeline --desde integracion
    .venv/bin/python -m scripts.run_pipeline --con-benchmark --con-escalabilidad

Cada etapa corre en su propio proceso (aislamiento: una JVM de Spark nueva por
etapa, como lo haría un orquestador) y su salida se escribe en
logs/pipeline_<fecha>.log. Si una etapa falla, el pipeline se detiene y lo
indica; como todas las etapas son idempotentes y escriben de forma atómica,
basta con relanzar desde esa etapa con --desde.

POR QUÉ un script y no Airflow: es un solo flujo mensual; un orquestador
completo sería desproporcionado (decisión registrada en la sección 2.4).
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from src.common.io_utils import get_logger, write_csv_atomic
from src.config import REPO_ROOT

# (nombre, módulo, descripción) en orden de dependencia.
ETAPAS = [
    ("ingesta_f1", "src.ingesta.fuente1_ordenes", "Fuente 1: descarga de CSV mensuales a raw + manifiesto"),
    ("limpieza_f1", "src.limpieza.fuente1_limpieza", "Fuente 1: limpieza, reglas de calidad, cuarentena y Parquet"),
    ("ingesta_f2_resoluciones", "src.ingesta.fuente2_resoluciones", "Fuente 2a: catálogo gob.pe y PDF de resoluciones"),
    ("ingesta_f2_ordenes_pdf", "src.ingesta.fuente2_ordenes_pdf", "Fuente 2b: PDF de órdenes MPT y verificación de enlaces"),
    ("extraccion_ordenes_pdf", "src.pdf.extraer_ordenes", "Fuente 2b: detalle por ítem desde los PDF"),
    ("extraccion_resoluciones", "src.pdf.extraer_resoluciones", "Fuente 2a: texto (OCR) y campos de las resoluciones"),
    ("verificacion_digitalizadas", "src.pdf.verificar_digitalizadas", "Fuente 2b: OCR de la muestra de órdenes digitalizadas"),
    ("integracion", "src.integracion.integrar_fuentes", "Integración CSV ⟷ PDF ⟷ resoluciones"),
    ("consumo", "src.consumo.construir_consumo", "Capa de consumo con seudonimización"),
]
SOLO = {"fuente1": ["ingesta_f1", "limpieza_f1", "consumo"]}
EXTRA = {"benchmark": ("benchmark", "scripts.benchmark_optimizacion", "Optimización medida (3.6)"),
         "escalabilidad": ("escalabilidad", "scripts.escalabilidad", "Prueba de escalabilidad x1/x10/x100 (3.7)")}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--solo", choices=sorted(SOLO))
    ap.add_argument("--desde", choices=[e[0] for e in ETAPAS])
    ap.add_argument("--con-benchmark", action="store_true")
    ap.add_argument("--con-escalabilidad", action="store_true")
    args = ap.parse_args()

    etapas = ETAPAS
    if args.solo:
        etapas = [e for e in ETAPAS if e[0] in SOLO[args.solo]]
    if args.desde:
        nombres = [e[0] for e in etapas]
        etapas = etapas[nombres.index(args.desde):] if args.desde in nombres else etapas
    if args.con_benchmark:
        etapas = etapas + [EXTRA["benchmark"]]
    if args.con_escalabilidad:
        etapas = etapas + [EXTRA["escalabilidad"]]

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = REPO_ROOT / "logs" / f"pipeline_{stamp}.log"
    log = get_logger("pipeline", log_path)
    log.info("Inicio del pipeline | etapas: %s", ", ".join(e[0] for e in etapas))
    log.info("Python %s | repo %s", sys.version.split()[0], REPO_ROOT)

    resumen, t_total = [], time.perf_counter()
    for nombre, modulo, desc in etapas:
        log.info("=" * 70)
        log.info("ETAPA %s — %s", nombre, desc)
        t0 = time.perf_counter()
        proc = subprocess.Popen([sys.executable, "-m", modulo], cwd=REPO_ROOT, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        for linea in proc.stdout:  # la salida de cada etapa queda en el log del pipeline
            linea = linea.rstrip()
            if linea and "WARN" not in linea and "setLogLevel" not in linea and "log level" not in linea:
                log.info("  [%s] %s", nombre, linea)
        proc.wait()
        dur = time.perf_counter() - t0
        estado = "OK" if proc.returncode == 0 else f"ERROR (código {proc.returncode})"
        resumen.append({"etapa": nombre, "descripcion": desc, "estado": estado, "segundos": round(dur, 1)})
        log.info("ETAPA %s terminó: %s en %.1f s", nombre, estado, dur)
        if proc.returncode != 0:
            log.error("Pipeline detenido. Corregir y relanzar con: --desde %s", nombre)
            break

    total = time.perf_counter() - t_total
    log.info("=" * 70)
    for r in resumen:
        log.info("%-26s %-8s %8.1f s", r["etapa"], r["estado"], r["segundos"])
    log.info("TOTAL %.1f s | log: %s", total, log_path)
    write_csv_atomic(REPO_ROOT / "logs" / f"pipeline_{stamp}_resumen.csv", resumen,
                     ["etapa", "descripcion", "estado", "segundos"])
    ok = all(r["estado"] == "OK" for r in resumen) and len(resumen) == len(etapas)
    if ok and not args.solo and not args.desde:
        # Evidencia de corrida completa para el informe (anexo 7).
        shutil.copy(log_path, REPO_ROOT / "docs" / "evidencias" / "corrida_completa.log")
        shutil.copy(REPO_ROOT / "logs" / f"pipeline_{stamp}_resumen.csv",
                    REPO_ROOT / "docs" / "evidencias" / "corrida_completa_resumen.csv")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
