"""Paso 7 — Prueba de escalabilidad (sección 3.7).

Replica el conjunto REAL x10 y x100 y ejecuta el mismo pipeline (limpieza +
calidad + escritura Parquet + capa de consumo con seudonimización) sobre cada
réplica, midiendo tiempo y memoria.

REGLA DEL CURSO: las réplicas son solo para medir rendimiento. Viven en
datalake/escalabilidad/ (zona separada), nunca alimentan el EDA ni el modelo,
y se borran al terminar (solo se conservan las métricas).

Cómo se replica (y por qué así):
* Cada CSV crudo se copia k veces con el MISMO formato (separador, codificación,
  columnas). Lo único que se altera es ORDEN_ELECTRÓNICA, a la que se agrega el
  sufijo "-R###" en las copias: si no, la regla de unicidad (R03) mandaría el
  99 % de la réplica a cuarentena y el trabajo medido sería otro.
* x1 usa la zona cruda real tal cual.

Cómo se mide:
* Cada factor corre en un PROCESO HIJO nuevo (JVM limpia, sin cachés de la
  corrida anterior). El padre muestrea cada 0,5 s la memoria residente (RSS)
  de todo el árbol de procesos del hijo (Python + JVM de Spark + workers) y
  registra el pico. También registra la carga del sistema.

Uso: .venv/bin/python -m scripts.escalabilidad [--factores 1 10 100] [--conservar]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from multiprocessing import Pool
from pathlib import Path

import psutil

from src.common.io_utils import dir_size_bytes, get_logger, read_csv_dicts, write_csv_atomic
from src.config import REPO_ROOT, load_config, zone_path

log = get_logger("escalabilidad", REPO_ROOT / "logs" / "escalabilidad.log")
COL_ORDEN = 6  # posición de ORDEN_ELECTRÓNICA en las 19 columnas


# ------------------------------------------------------------- réplica
def _replicar_archivo(args) -> list[dict]:
    origen, destino_dir, m, k = args
    data = Path(origen).read_bytes()
    lineas = data.split(b"\n")
    cabecera, filas = lineas[0], [l for l in lineas[1:] if l.strip()]
    salida = []
    destino_dir = Path(destino_dir)
    destino_dir.mkdir(parents=True, exist_ok=True)
    for j in range(k):
        nombre = f"{Path(m['nombre_archivo']).stem}__r{j:03d}.csv"
        if j == 0:
            cuerpo = filas
        else:
            sufijo = f"-R{j:03d}".encode()
            cuerpo = []
            for l in filas:
                campos = l.split(b";")
                campos[COL_ORDEN] = campos[COL_ORDEN].rstrip() + sufijo
                cuerpo.append(b";".join(campos))
        (destino_dir / nombre).write_bytes(cabecera + b"\n" + b"\n".join(cuerpo) + b"\n")
        salida.append({**m, "nombre_archivo": nombre, "filas": len(filas),
                       "bytes": (destino_dir / nombre).stat().st_size, "sha256": ""})
    return salida


def generar_replica(raw_real: Path, destino: Path, k: int) -> tuple[int, int]:
    manifest = read_csv_dicts(raw_real / "_manifiesto.csv")
    tareas = [(str(raw_real / f"anio={m['anio']}" / f"mes={m['mes']}" / m["nombre_archivo"]),
               str(destino / f"anio={m['anio']}" / f"mes={m['mes']}"), m, k) for m in manifest]
    with Pool(min(8, os.cpu_count() or 4)) as pool:
        filas = [f for parte in pool.map(_replicar_archivo, tareas) for f in parte]
    write_csv_atomic(destino / "_manifiesto.csv", filas, list(filas[0].keys()))
    return sum(int(f["filas"]) for f in filas), sum(int(f["bytes"]) for f in filas)


# ------------------------------------------------------- corrida (hijo)
def ejecutar_factor(k: int, raw_root: str, salida_root: str) -> None:
    """Se ejecuta en el proceso hijo: el pipeline de siempre sobre la réplica."""
    from src.consumo.construir_consumo import construir_ordenes
    from src.limpieza.fuente1_limpieza import ejecutar
    from src.seguridad.seudonimizacion import obtener_clave
    from src.spark_session import get_spark

    cfg = load_config()
    spark = get_spark(cfg, f"escalabilidad-x{k}")
    t0 = time.perf_counter()
    m = ejecutar(spark, cfg, Path(raw_root), f"{salida_root}/processed", f"{salida_root}/cuarentena", None,
                 get_logger(f"escalabilidad.x{k}"))
    t1 = time.perf_counter()
    c = construir_ordenes(spark, f"{salida_root}/processed", f"{salida_root}/consumo",
                          obtener_clave(cfg["seguridad"]["salt_env_var"]))
    t2 = time.perf_counter()
    spark.stop()
    print("RESULTADO_JSON " + json.dumps({"registros_leidos": m["registros_leidos"], "registros_validos": m["registros_validos"],
                                          "registros_cuarentena": m["registros_cuarentena"],
                                          "seg_limpieza_calidad": round(t1 - t0, 2), "seg_consumo": round(t2 - t1, 2),
                                          "ordenes_consumo": c["ordenes_nacional"], "pn_en_claro": c["pn_en_claro"]}))


def medir_hijo(k: int, raw_root: Path, salida_root: Path, tmp_spark: Path) -> dict:
    env = {**os.environ, "SPARK_LOCAL_DIRS": str(tmp_spark)}  # derrames de shuffle dentro de la zona de pruebas
    cmd = [sys.executable, "-m", "scripts.escalabilidad", "--hijo", str(k), str(raw_root), str(salida_root)]
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    pico, cargas, vivo = [0], [], [True]

    def muestrear():
        p = psutil.Process(proc.pid)
        while vivo[0]:
            try:
                rss = p.memory_info().rss + sum(c.memory_info().rss for c in p.children(recursive=True))
                pico[0] = max(pico[0], rss)
                cargas.append(os.getloadavg()[0])
            except psutil.Error:
                pass
            time.sleep(0.5)

    hilo = threading.Thread(target=muestrear, daemon=True)
    hilo.start()
    salida = proc.stdout.read()
    proc.wait()
    vivo[0] = False
    hilo.join()
    dur = time.perf_counter() - t0
    if proc.returncode != 0:
        log.error("La corrida x%d falló:\n%s", k, salida[-3000:])
        raise RuntimeError(f"Falló x{k}")
    res = json.loads(next(l for l in salida.splitlines() if l.startswith("RESULTADO_JSON "))[15:])
    res.update({"seg_total_proceso": round(dur, 2), "pico_memoria_mb": round(pico[0] / 1024 / 1024),
                "carga_sistema_media": round(sum(cargas) / len(cargas), 1) if cargas else None})
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--factores", type=int, nargs="+", default=[1, 10, 100])
    ap.add_argument("--conservar", action="store_true", help="no borrar las réplicas al terminar")
    ap.add_argument("--hijo", nargs=3, metavar=("K", "RAW", "SALIDA"))
    args = ap.parse_args()
    if args.hijo:
        ejecutar_factor(int(args.hijo[0]), args.hijo[1], args.hijo[2])
        return

    cfg = load_config()
    raw_real = Path(zone_path(cfg, "raw", "ordenes_compra"))
    base = Path(zone_path(cfg, "escalabilidad"))
    base.mkdir(parents=True, exist_ok=True)
    filas = []
    for k in args.factores:
        libre_gb = shutil.disk_usage(base).free / 1e9
        necesario_gb = 0.27 * k * 1.6  # CSV + Parquet de salida + derrames, estimado con x1
        if libre_gb < necesario_gb + 10:
            log.error("x%d necesita ~%.0f GB y quedan %.0f GB libres: se omite para no llenar el disco", k, necesario_gb, libre_gb)
            filas.append({"factor": k, "estado": f"OMITIDO: disco insuficiente ({libre_gb:.0f} GB libres)"})
            continue
        raw_k = raw_real if k == 1 else base / f"raw_x{k}" / "ordenes_compra"
        t0 = time.perf_counter()
        if k != 1 and not (raw_k / "_manifiesto.csv").exists():
            n, b = generar_replica(raw_real, raw_k, k)
            log.info("Réplica x%d generada: %d filas, %.2f GB en %.0f s", k, n, b / 1e9, time.perf_counter() - t0)
        manifest = read_csv_dicts(raw_k / "_manifiesto.csv")
        bytes_in = sum(int(m["bytes"]) for m in manifest)
        salida = base / f"salida_x{k}"
        tmp_spark = base / "_spark_tmp"
        tmp_spark.mkdir(exist_ok=True)
        log.info("Ejecutando pipeline x%d (%d archivos, %.2f GB)...", k, len(manifest), bytes_in / 1e9)
        r = medir_hijo(k, raw_k, salida, tmp_spark)
        r.update({"factor": k, "estado": "OK", "archivos_csv": len(manifest), "gb_entrada_csv": round(bytes_in / 1e9, 3),
                  "gb_salida_parquet": round(dir_size_bytes(salida / "processed") / 1e9, 3),
                  "registros_por_segundo": round(r["registros_leidos"] / r["seg_total_proceso"])})
        log.info("x%d: %s", k, r)
        filas.append(r)
        if not args.conservar:
            shutil.rmtree(salida, ignore_errors=True)
            shutil.rmtree(tmp_spark, ignore_errors=True)
            if k != 1:
                shutil.rmtree(raw_k.parent, ignore_errors=True)

    base_ok = next((f for f in filas if f.get("factor") == 1 and f.get("estado") == "OK"), None)
    for f in filas:
        if base_ok and f.get("estado") == "OK":
            f["tiempo_relativo_vs_x1"] = round(f["seg_total_proceso"] / base_ok["seg_total_proceso"], 2)
            f["eficiencia_escalado"] = round(f["factor"] / f["tiempo_relativo_vs_x1"], 2)  # 1.0 = crecimiento lineal
    campos = ["factor", "estado", "archivos_csv", "registros_leidos", "registros_validos", "registros_cuarentena",
              "gb_entrada_csv", "gb_salida_parquet", "seg_limpieza_calidad", "seg_consumo", "seg_total_proceso",
              "tiempo_relativo_vs_x1", "eficiencia_escalado", "registros_por_segundo", "pico_memoria_mb",
              "carga_sistema_media", "ordenes_consumo", "pn_en_claro"]
    write_csv_atomic(REPO_ROOT / "docs" / "evidencias" / "escalabilidad.csv", filas, campos)
    log.info("Tabla de escalabilidad escrita en docs/evidencias/escalabilidad.csv")


if __name__ == "__main__":
    main()
