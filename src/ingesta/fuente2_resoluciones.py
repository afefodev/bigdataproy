"""Paso 8 (ingesta) — Fuente 2: resoluciones PDF de la Municipalidad Provincial de Trujillo.

Hallazgo que define el diseño: munitrujillo.gob.pe redirige a la Plataforma del
Estado (gob.pe/munitrujillo), donde la MPT publica sus normas legales. gob.pe
ofrece un buscador con respuesta JSON (`/busquedas.json`) que devuelve por cada
norma su número, fecha de publicación, resumen y enlace al PDF. Se usa esa
interfaz en lugar de "raspar" HTML porque es más estable y trae los metadatos
estructurados.

Etapas:
1. Catálogo: se recorren TODAS las páginas del buscador (normas de la MPT) y se
   guarda la respuesta completa en raw/resoluciones_mpt/_catalogo/ (evidencia de
   qué existía el día de la descarga).
2. Selección: se eligen las normas vinculadas a abastecimiento con reglas
   declaradas en config.yaml (unidad emisora y palabras clave), dentro del
   período de la Fuente 1. La regla es explícita para que sea auditable.
3. Descarga: cada PDF se guarda SIN modificar en
   raw/resoluciones_mpt/anio=AAAA/mes=MM/<archivo>.pdf con su manifiesto
   (nombre, URL, bytes, SHA-256, páginas, fecha de descarga).

Uso: .venv/bin/python -m src.ingesta.fuente2_resoluciones
"""
from __future__ import annotations

import json
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import pdfplumber
import requests

from src.common.io_utils import (
    atomic_replace, get_logger, read_csv_dicts, sha256_file, write_csv_atomic,
)
from src.config import REPO_ROOT, load_config, zone_path

FUENTE = "resoluciones_mpt"
UA = {"User-Agent": "preciojusto-ingesta/1.0 (proyecto academico UPAO)"}
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7,
         "agosto": 8, "setiembre": 9, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
MANIFEST_FIELDS = ["anio", "mes", "id_norma", "numero", "titulo", "fecha_publicacion",
                   "nombre_archivo", "url", "bytes", "sha256", "paginas", "motivo_seleccion",
                   "fecha_descarga_utc"]


def sin_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def fecha_es_a_iso(txt: str) -> str | None:
    """'22 de setiembre de 2025' -> '2025-09-22' (formato que publica gob.pe)."""
    m = re.search(r"(\d{1,2}) de ([a-záéíóú]+) de (\d{4})", (txt or "").strip().lower())
    if not m or m.group(2) not in MESES:
        return None
    return f"{int(m.group(3)):04d}-{MESES[m.group(2)]:02d}-{int(m.group(1)):02d}"


def catalogar(cfg_src: dict, log) -> list[dict]:
    """Recorre todas las páginas del buscador de normas de la MPT."""
    url = "https://www.gob.pe/busquedas.json"
    params = {"contenido[]": "normas", "institucion[]": cfg_src["institucion_gobpe"], "sort_by": "recent"}
    resultados, sheet, total = [], 1, None
    while True:
        r = requests.get(url, params={**params, "sheet": sheet}, headers=UA, timeout=60)
        r.raise_for_status()
        attrs = r.json()["data"]["attributes"]
        total = attrs["total_count"]
        pagina = attrs.get("results") or []
        if not pagina:
            break
        resultados.extend(pagina)
        sheet += 1
        time.sleep(cfg_src["pausa_s"])  # cortesía con el servidor público
    log.info("Catálogo gob.pe: %d normas declaradas, %d recibidas en %d páginas", total, len(resultados), sheet - 1)
    return resultados


def seleccionar(catalogo: list[dict], cfg_src: dict) -> list[dict]:
    """Regla de selección declarada (config.yaml > sources.resoluciones_mpt.seleccion).

    Una norma entra si (a) su número indica una unidad emisora del sistema
    administrativo/abastecimiento (p. ej. ...-MPT-OGAF, ...-MPT-GAF), o
    (b) su título/resumen contiene alguna palabra clave de abastecimiento;
    y además su fecha de publicación cae en el período de la Fuente 1.
    """
    sel_cfg = cfg_src["seleccion"]
    unidades = [u.upper() for u in sel_cfg["unidades_emisoras"]]
    claves = [sin_tildes(k.lower()) for k in sel_cfg["palabras_clave"]]
    elegidas, vistos = [], set()
    for n in catalogo:
        numero = (n.get("name_with_parent") or "").upper()
        texto = sin_tildes(re.sub(r"<[^>]+>", "", f"{n.get('subject','')} {n.get('content','')}").lower())
        fecha = fecha_es_a_iso(n.get("publication", ""))
        if not fecha or not (sel_cfg["desde"] <= fecha[:7] <= sel_cfg["hasta"]):
            continue
        if not (n.get("action_url") or "").lower().split("?")[0].endswith(".pdf"):
            continue
        motivo = None
        unidad = next((u for u in unidades if re.search(rf"-{re.escape(u)}\b", numero)), None)
        if unidad:
            motivo = f"unidad:{unidad}"
        else:
            clave = next((k for k in claves if k in texto), None)
            if clave:
                motivo = f"clave:{clave}"
        if motivo and n["id"] not in vistos:
            vistos.add(n["id"])
            elegidas.append({**n, "_fecha": fecha, "_motivo": motivo})
    elegidas.sort(key=lambda x: x["_fecha"], reverse=True)
    return elegidas[: cfg_src["max_documentos"]]


def descargar(url: str, destino: Path, log, reintentos: int = 3) -> None:
    tmp = destino.with_name(destino.name + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    for i in range(1, reintentos + 1):
        try:
            with requests.get(url, headers=UA, timeout=120, stream=True) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(1 << 16):
                        f.write(chunk)
            atomic_replace(tmp, destino)
            return
        except requests.RequestException as e:
            log.warning("Intento %d/%d falló (%s): %s", i, reintentos, url, e)
            time.sleep(2 * i)
    tmp.unlink(missing_ok=True)
    raise RuntimeError(f"No se pudo descargar {url}")


def main() -> dict:
    cfg = load_config()
    src = cfg["sources"][FUENTE]
    log = get_logger("ingesta.f2", REPO_ROOT / "logs" / "ingesta_fuente2.log")
    raw_root = Path(zone_path(cfg, "raw", FUENTE))
    manifest_path = raw_root / "_manifiesto.csv"
    manifest = {m["id_norma"]: m for m in read_csv_dicts(manifest_path)}

    catalogo = catalogar(src, log)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cat_dir = raw_root / "_catalogo"
    cat_dir.mkdir(parents=True, exist_ok=True)
    (cat_dir / f"catalogo_gobpe_{stamp}.json").write_text(json.dumps(catalogo, ensure_ascii=False, indent=1), encoding="utf-8")

    elegidas = seleccionar(catalogo, src)
    log.info("Seleccionadas por regla de abastecimiento: %d", len(elegidas))

    nuevos = 0
    for n in elegidas:
        nid = str(n["id"])
        anio, mes = n["_fecha"][:4], n["_fecha"][5:7]
        nombre = unquote(Path(urlparse(n["action_url"]).path).name)
        destino = raw_root / f"anio={anio}" / f"mes={mes}" / nombre
        if destino.exists() and nid in manifest:
            if sha256_file(destino) != manifest[nid]["sha256"]:
                raise RuntimeError(f"El PDF crudo {destino} cambió: la zona cruda debe ser inmutable")
            continue
        if not destino.exists():
            descargar(n["action_url"], destino, log)
            time.sleep(src["pausa_s"])
        try:
            with pdfplumber.open(destino) as pdf:
                paginas = len(pdf.pages)
        except Exception as e:  # PDF dañado: se registra, no se oculta
            log.warning("No se pudo abrir %s: %s", destino.name, e)
            paginas = -1
        manifest[nid] = {
            "anio": anio, "mes": mes, "id_norma": nid, "numero": n.get("name_with_parent", ""),
            "titulo": n.get("subject", ""), "fecha_publicacion": n["_fecha"], "nombre_archivo": nombre,
            "url": n["action_url"], "bytes": destino.stat().st_size, "sha256": sha256_file(destino),
            "paginas": paginas, "motivo_seleccion": n["_motivo"],
            "fecha_descarga_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        write_csv_atomic(manifest_path, sorted(manifest.values(), key=lambda x: x["fecha_publicacion"]), MANIFEST_FIELDS)
        nuevos += 1
        log.info("%s %-28s %3s págs %9d bytes [%s]", n["_fecha"], n.get("name_with_parent", ""), paginas,
                 destino.stat().st_size, n["_motivo"])

    resumen = {"catalogo": len(catalogo), "seleccionadas": len(elegidas), "pdf_en_manifiesto": len(manifest),
               "nuevos": nuevos, "bytes": sum(int(m["bytes"]) for m in manifest.values()),
               "paginas": sum(max(int(m["paginas"]), 0) for m in manifest.values())}
    log.info("RESUMEN ingesta F2: %s", resumen)
    return resumen


if __name__ == "__main__":
    main()
