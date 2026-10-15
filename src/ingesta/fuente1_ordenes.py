"""Paso 3 — Ingesta de la Fuente 1 (órdenes de compra de Acuerdos Marco, bienes).

Qué hace:
1. Consulta la API del portal (CKAN `package_show`) para obtener la lista
   oficial de recursos del dataset, en lugar de adivinar URLs: el portal a veces
   publica archivos con sufijo (`ReportePCBienes202302_0.csv`), así que un patrón
   fijo de URL fallaría.
2. Descarga cada CSV mensual del rango configurado a
   raw/ordenes_compra/anio=AAAA/mes=MM/<nombre original>, SIN modificarlo.
3. Registra cada archivo en un manifiesto: nombre, URL, bytes, SHA-256, filas
   y fecha de descarga (evidencia para la ficha de procedencia 3.1).

Idempotencia: si el archivo ya está en la zona cruda y su hash coincide con el
manifiesto, no se vuelve a descargar. Relanzar el job no duplica nada.
Inmutabilidad: un archivo crudo existente nunca se sobrescribe.

Uso: .venv/bin/python -m src.ingesta.fuente1_ordenes
"""
from __future__ import annotations

import csv
import io
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.common.io_utils import (
    atomic_replace, get_logger, read_csv_dicts, sha256_file, write_csv_atomic,
)
from src.config import REPO_ROOT, load_config, zone_path

FUENTE = "ordenes_compra"
MANIFEST_FIELDS = [
    "anio", "mes", "nombre_archivo", "url", "recurso_id", "bytes", "sha256",
    "filas", "codificacion", "fecha_descarga_utc", "recurso_creado_portal",
]
# Los nombres de recurso del portal son "Órdenes de Compra 2022-01" (con
# mayúsculas/minúsculas variables); de ahí se toma el período.
RE_PERIODO = re.compile(r"(\d{4})-(\d{2})\s*$")
# Codificación: verificada archivo por archivo. Perú Compras NO es consistente:
# unos meses vienen en cp1252 (Windows-1252) y otros en UTF-8. Por eso se
# detecta y se guarda en el manifiesto; la limpieza lee cada archivo con la suya.
ENCODINGS_CANDIDATAS = ("utf-8", "cp1252")


def detectar_codificacion(path: Path) -> str:
    """UTF-8 estricto primero: un archivo cp1252 con tildes casi nunca es UTF-8
    válido, mientras que cp1252 'acepta' bytes UTF-8 y produciría mojibake
    (ej. 'Ã' en lugar de 'Ó'). Así el orden de prueba evita falsos positivos."""
    data = path.read_bytes()
    for enc in ENCODINGS_CANDIDATAS:
        try:
            data.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Codificación no reconocida en {path}")


def obtener_recursos(cfg_src: dict, log) -> tuple[list[dict], dict]:
    url = f"{cfg_src['portal']}/api/3/action/package_show?id={cfg_src['dataset_id']}"
    r = requests.get(url, timeout=cfg_src["timeout_s"], headers={"User-Agent": "preciojusto-ingesta/1.0"})
    r.raise_for_status()
    pkg = r.json()["result"][0]
    log.info("Dataset: %s | recursos: %d", pkg["title"], len(pkg["resources"]))
    return pkg["resources"], pkg


def contar_filas(path: Path, encoding: str) -> int:
    """Cuenta registros con el lector CSV (no líneas), por si algún campo trae
    saltos de línea entre comillas."""
    with open(path, encoding=encoding, newline="") as f:
        return sum(1 for _ in csv.reader(f, delimiter=";")) - 1


def descargar(url: str, destino: Path, cfg_src: dict, log) -> None:
    tmp = destino.with_name(destino.name + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    for intento in range(1, cfg_src["reintentos"] + 1):
        try:
            with requests.get(url, stream=True, timeout=cfg_src["timeout_s"],
                              headers={"User-Agent": "preciojusto-ingesta/1.0"}) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(1 << 16):
                        f.write(chunk)
            atomic_replace(tmp, destino)
            return
        except requests.RequestException as e:
            log.warning("Intento %d/%d falló para %s: %s", intento, cfg_src["reintentos"], url, e)
            time.sleep(2 * intento)  # espera creciente para no saturar el portal
    tmp.unlink(missing_ok=True)
    raise RuntimeError(f"No se pudo descargar {url}")


def main() -> dict:
    cfg = load_config()
    src = cfg["sources"][FUENTE]
    log = get_logger("ingesta.f1", REPO_ROOT / "logs" / "ingesta_fuente1.log")
    raw_root = Path(zone_path(cfg, "raw", FUENTE))
    manifest_path = raw_root / "_manifiesto.csv"
    manifest = {m["nombre_archivo"]: m for m in read_csv_dicts(manifest_path)}

    recursos, pkg = obtener_recursos(src, log)

    # Metadatos del dataset tal como los publica el portal (procedencia).
    meta_dir = raw_root / "_metadatos"
    meta_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (meta_dir / f"package_show_{stamp}.json").write_text(json.dumps(pkg, ensure_ascii=False, indent=1), encoding="utf-8")

    # Diccionario de datos oficial (PDF): se guarda junto a los datos.
    for r in recursos:
        if (r.get("format") or "").lower() == "pdf":
            dest = meta_dir / Path(r["url"]).name
            if not dest.exists():
                descargar(r["url"], dest, src, log)
                log.info("Diccionario descargado: %s", dest.name)

    desde, hasta = src["desde"], src["hasta"]
    nuevos = omitidos = 0
    for r in recursos:
        if (r.get("format") or "").lower() != "csv":
            continue
        m = RE_PERIODO.search(r["name"])
        if not m:
            log.warning("Recurso sin período reconocible, se ignora: %s", r["name"])
            continue
        anio, mes = m.group(1), m.group(2)
        if not (desde <= f"{anio}-{mes}" <= hasta):
            continue
        nombre = Path(r["url"]).name
        destino = raw_root / f"anio={anio}" / f"mes={mes}" / nombre

        if destino.exists() and nombre in manifest:
            # Idempotencia + verificación de integridad de la zona cruda.
            if sha256_file(destino) != manifest[nombre]["sha256"]:
                raise RuntimeError(f"El archivo crudo {destino} cambió: la zona cruda debe ser inmutable")
            omitidos += 1
            continue

        t0 = time.perf_counter()
        if not destino.exists():
            descargar(r["url"], destino, src, log)
        enc = detectar_codificacion(destino)
        filas = contar_filas(destino, enc)
        manifest[nombre] = {
            "anio": anio, "mes": mes, "nombre_archivo": nombre, "url": r["url"],
            "recurso_id": r.get("id", ""), "bytes": destino.stat().st_size,
            "sha256": sha256_file(destino), "filas": filas, "codificacion": enc,
            "fecha_descarga_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "recurso_creado_portal": r.get("created", ""),
        }
        # El manifiesto se reescribe tras cada archivo: si el job se corta,
        # lo ya descargado queda registrado y no se repite.
        write_csv_atomic(manifest_path, sorted(manifest.values(), key=lambda x: (x["anio"], x["mes"])), MANIFEST_FIELDS)
        nuevos += 1
        log.info("%s-%s %-32s %8d filas %10d bytes %-6s (%.1f s)", anio, mes, nombre, filas,
                 destino.stat().st_size, enc, time.perf_counter() - t0)

    filas_tot = sum(int(m["filas"]) for m in manifest.values())
    bytes_tot = sum(int(m["bytes"]) for m in manifest.values())
    resumen = {"archivos": len(manifest), "nuevos": nuevos, "omitidos": omitidos,
               "filas": filas_tot, "bytes": bytes_tot, "MB": round(bytes_tot / 1_000_000, 2),
               "MiB": round(bytes_tot / 1024 / 1024, 2)}
    log.info("RESUMEN ingesta F1: %s", resumen)
    return resumen


if __name__ == "__main__":
    main()
