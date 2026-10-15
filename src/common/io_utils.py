"""Utilidades de E/S compartidas: hash, escritura atómica, manifiestos y log.

POR QUÉ escritura atómica: si un job se corta a la mitad (red, Ctrl+C, falta
de disco) no debe quedar un archivo a medio escribir con el nombre definitivo,
porque la siguiente corrida lo tomaría como válido. Se escribe en `*.tmp` y se
renombra al final; el renombrado dentro del mismo sistema de archivos es
atómico (o existe el archivo completo o no existe).
"""
from __future__ import annotations

import csv
import hashlib
import logging
import os
import shutil
import sys
from pathlib import Path

LOG_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str, log_file: str | os.PathLike | None = None) -> logging.Logger:
    """Logger a consola y, opcionalmente, a archivo (evidencia de ejecución)."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(LOG_FMT))
    logger.addHandler(h)
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(LOG_FMT))
        logger.addHandler(fh)
    logger.propagate = False
    return logger


def sha256_file(path: str | os.PathLike, chunk: int = 1 << 20) -> str:
    """SHA-256 del archivo: prueba de integridad y de que la zona cruda no cambió."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def atomic_replace(tmp_path: str | os.PathLike, final_path: str | os.PathLike) -> None:
    """Renombra el temporal al nombre final (atómico en el mismo disco)."""
    Path(final_path).parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp_path, final_path)


def atomic_replace_dir(tmp_dir: str | os.PathLike, final_dir: str | os.PathLike) -> None:
    """Sustituye un directorio completo (salida de Spark) por su versión nueva.

    Spark escribe a `<destino>.__tmp`; solo si terminó bien se mueve el destino
    viejo a `.__old`, se renombra el nuevo y se borra el viejo. Si algo falla
    antes del renombrado, el destino anterior sigue intacto (job relanzable).
    """
    tmp_dir, final_dir = Path(tmp_dir), Path(final_dir)
    old = final_dir.with_name(final_dir.name + ".__old")
    if old.exists():
        shutil.rmtree(old)
    if final_dir.exists():
        os.replace(final_dir, old)
    os.replace(tmp_dir, final_dir)
    if old.exists():
        shutil.rmtree(old)


def write_csv_atomic(path: str | os.PathLike, rows: list[dict], fieldnames: list[str]) -> None:
    """Escribe un CSV (p. ej. el manifiesto) de forma atómica, en UTF-8."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    atomic_replace(tmp, path)


def read_csv_dicts(path: str | os.PathLike) -> list[dict]:
    if not Path(path).exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def dir_size_bytes(path: str | os.PathLike) -> int:
    return sum(p.stat().st_size for p in Path(path).rglob("*") if p.is_file())
