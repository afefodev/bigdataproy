"""Paso 8 (ingesta) — Fuente 2b: PDF de las órdenes de compra de la MPT.

Cada orden del CSV de Perú Compras trae dos enlaces a documentos:
  ORDEN_ELECTRÓNICA_GENERADA  PDF generado por el sistema (texto nativo) con el
                              DETALLE POR ÍTEM: producto, marca, cantidad y
                              precio unitario — justo lo que el CSV no tiene.
  ORDEN_DIGITALIZADA          copia escaneada y firmada que sube la entidad (imagen).
y algunas un tercer enlace (DOCUMENTO_ESTADO_OCAM) con el sustento de un cambio de estado.

Qué hace este job:
1. Descarga el PDF "generado" de TODAS las órdenes de la MPT (entidad cliente).
2. Descarga una muestra de órdenes "digitalizadas" de la MPT (para medir si el
   OCR aporta algo) y verifica una muestra nacional de DOCUMENTO_ESTADO (la MPT
   no tiene ninguno).
3. Registra, para cada enlace, código HTTP, tipo de contenido, bytes y SHA-256
   en un manifiesto: es la evidencia de "¿los enlaces funcionan?".

Los PDF contienen datos personales (representante legal, correos, teléfonos,
DNI del responsable de recepción). Por eso se quedan SOLO en la zona cruda, de
acceso restringido; la extracción toma únicamente los campos no personales.

Uso: .venv/bin/python -m src.ingesta.fuente2_ordenes_pdf
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.dataset as ds
import requests

from src.common.io_utils import atomic_replace, get_logger, read_csv_dicts, sha256_file, write_csv_atomic
from src.config import REPO_ROOT, load_config, zone_path

FUENTE = "ordenes_pdf"
UA = {"User-Agent": "preciojusto-ingesta/1.0 (proyecto academico UPAO)"}
CAMPOS = ["tipo_documento", "orden_electronica", "anio", "mes", "url", "http_status", "content_type",
          "bytes", "sha256", "es_pdf", "ruta_relativa", "error", "fecha_descarga_utc"]
SEMILLA = 20261011  # muestras reproducibles


def bajar(url: str, destino: Path, pausa: float) -> dict:
    """Descarga atómica; devuelve el resultado de la verificación del enlace."""
    res = {"http_status": "", "content_type": "", "bytes": 0, "sha256": "", "es_pdf": False, "error": ""}
    tmp = destino.with_name(destino.name + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    for intento in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=90)
            res["http_status"], res["content_type"] = r.status_code, r.headers.get("content-type", "")
            if r.status_code == 200:
                tmp.write_bytes(r.content)
                res["es_pdf"] = r.content[:5] == b"%PDF-"
                if res["es_pdf"]:
                    atomic_replace(tmp, destino)
                    res["bytes"], res["sha256"] = destino.stat().st_size, sha256_file(destino)
                else:
                    tmp.unlink(missing_ok=True)
                    res["error"] = "respuesta 200 pero no es PDF"
            else:
                res["error"] = f"HTTP {r.status_code}"
            break
        except requests.RequestException as e:
            res["error"] = type(e).__name__
            time.sleep(2 * (intento + 1))
    time.sleep(pausa)
    return res


def guardar_manifiesto(path: Path, previo: dict, filas: list[dict]) -> None:
    """El manifiesto conserva TODO lo descargado alguna vez (zona cruda
    inmutable: ningún archivo queda huérfano), actualizado con esta corrida."""
    actual = {(f["tipo_documento"], f["orden_electronica"]): f for f in filas}
    todo = {**previo, **actual}
    write_csv_atomic(path, sorted(todo.values(), key=lambda f: (f["tipo_documento"], f["orden_electronica"])), CAMPOS)


def main() -> dict:
    cfg = load_config()
    src = cfg["sources"][FUENTE]
    log = get_logger("ingesta.f2b", REPO_ROOT / "logs" / "ingesta_fuente2_ordenes_pdf.log")
    raw_root = Path(zone_path(cfg, "raw", FUENTE))
    manifest_path = raw_root / "_manifiesto.csv"
    previo = {(m["tipo_documento"], m["orden_electronica"]): m for m in read_csv_dicts(manifest_path)}

    # Lista de órdenes desde la zona procesada (contrato de la Fuente 1), sin Spark:
    # PyArrow lee solo las columnas y filas necesarias del Parquet.
    proc = ds.dataset(zone_path(cfg, "processed", "ordenes_compra", "v1"), format="parquet", partitioning="hive")
    cols = ["orden_electronica", "anio", "mes", "url_orden_generada", "url_orden_digitalizada", "url_documento_estado"]
    mpt = proc.to_table(columns=cols, filter=ds.field("es_mpt") == True).to_pylist()  # noqa: E712
    con_doc = proc.to_table(columns=cols, filter=ds.field("flag_documento_estado") == True).to_pylist()  # noqa: E712
    # Orden fijo antes de muestrear: el orden de filas de un Parquet no está
    # garantizado y sin esto la "misma" semilla daba otra muestra (detectado al
    # relanzar el pipeline completo).
    mpt.sort(key=lambda o: o["orden_electronica"])
    con_doc.sort(key=lambda o: o["orden_electronica"])
    rnd = random.Random(SEMILLA)
    tareas = [("generada", o, o["url_orden_generada"]) for o in mpt]
    tareas += [("digitalizada", o, o["url_orden_digitalizada"])
               for o in rnd.sample(mpt, min(src["muestra_verificacion"], len(mpt)))]
    tareas += [("documento_estado", o, o["url_documento_estado"])
               for o in rnd.sample(con_doc, min(src["muestra_verificacion"], len(con_doc)))]
    log.info("Órdenes MPT: %d | tareas de descarga/verificación: %d", len(mpt), len(tareas))

    filas, t0 = [], time.perf_counter()
    for i, (tipo, o, url) in enumerate(tareas, 1):
        clave = (tipo, o["orden_electronica"])
        rel = f"{tipo}/anio={o['anio']}/mes={int(o['mes']):02d}/{o['orden_electronica']}.pdf"
        destino = raw_root / rel
        if clave in previo and previo[clave]["es_pdf"] == "True" and destino.exists():
            if sha256_file(destino) != previo[clave]["sha256"]:
                raise RuntimeError(f"PDF crudo modificado: {destino}")
            filas.append(previo[clave])  # idempotente: ya descargado y verificado
            continue
        res = bajar(url, destino, src["pausa_s"])
        filas.append({"tipo_documento": tipo, "orden_electronica": o["orden_electronica"], "anio": o["anio"],
                      "mes": o["mes"], "url": url, **res, "ruta_relativa": rel if res["es_pdf"] else "",
                      "fecha_descarga_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        if i % 50 == 0 or i == len(tareas):
            guardar_manifiesto(manifest_path, previo, filas)
            log.info("%d/%d (%.0f s)", i, len(tareas), time.perf_counter() - t0)
    guardar_manifiesto(manifest_path, previo, filas)

    resumen = {}
    for tipo in ("generada", "digitalizada", "documento_estado"):
        sub = [f for f in filas if f["tipo_documento"] == tipo]
        ok = [f for f in sub if str(f["es_pdf"]) == "True"]
        resumen[tipo] = {"enlaces": len(sub), "pdf_ok": len(ok), "fallidos": len(sub) - len(ok),
                         "bytes": sum(int(f["bytes"] or 0) for f in ok)}
    log.info("RESUMEN ingesta F2b: %s", resumen)
    # Evidencia de verificación de enlaces (sin datos personales: solo código de orden y estado HTTP)
    write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "verificacion_enlaces.csv",
                     [{k: f[k] for k in ("tipo_documento", "orden_electronica", "http_status", "content_type",
                                         "bytes", "es_pdf", "error")} for f in filas if f["tipo_documento"] != "generada"],
                     ["tipo_documento", "orden_electronica", "http_status", "content_type", "bytes", "es_pdf", "error"])
    return resumen


if __name__ == "__main__":
    main()
