"""Carga de configuración compartida por todos los jobs.

POR QUÉ existe este módulo: ningún script debe tener rutas ni credenciales
escritas a mano. Todos piden la ruta de una zona a `zone_path()`, de modo que
pasar de disco local a HDFS (o S3) es cambiar `base_path` en el YAML.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

# .env guarda JAVA_HOME y la sal; se carga antes de crear la sesión de Spark.
load_dotenv(REPO_ROOT / ".env")


def load_config(path: str | os.PathLike | None = None) -> dict:
    """Lee config/config.yaml; si no existe, usa el de ejemplo (y lo avisa)."""
    if path is None:
        path = REPO_ROOT / "config" / "config.yaml"
        if not Path(path).exists():
            print("[config] config/config.yaml no existe; usando config.example.yaml")
            path = REPO_ROOT / "config" / "config.example.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def base_path(cfg: dict) -> str:
    """Raíz del data lake. Si es relativa y local, se resuelve desde el repo."""
    base = cfg["base_path"]
    if "://" in base or os.path.isabs(base):
        return base.rstrip("/")
    return str(REPO_ROOT / base)


def zone_path(cfg: dict, zone: str, *parts: str) -> str:
    """Ruta a una zona del lake: zone_path(cfg, 'raw', 'ordenes_compra')."""
    return "/".join([base_path(cfg), cfg["zones"][zone], *parts])
