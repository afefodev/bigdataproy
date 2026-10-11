"""Figuras del informe (secciones 3.6 y 3.7) a partir de las evidencias reales.

Lee docs/evidencias/optimizacion.csv y escalabilidad.csv (producidos por los
scripts de medición) y genera PNG en docs/figuras/. No inventa ni suaviza datos:
cada punto o barra es una fila de esos CSV.

Uso: .venv/bin/python -m scripts.figuras_informe
"""
from __future__ import annotations

import csv

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from src.config import REPO_ROOT  # noqa: E402

EVID = REPO_ROOT / "docs" / "evidencias"
OUT = REPO_ROOT / "docs" / "figuras"
AZUL = "#2a78d6"      # serie 1 de la paleta de referencia (única serie de color)
TINTA = "#0b0b0b"
TINTA_2 = "#52514e"   # texto secundario y línea de referencia neutra
GRILLA = "#e4e3df"

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": GRILLA,
                     "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
                     "axes.spines.top": False, "axes.spines.right": False})


def leer(nombre):
    with open(EVID / nombre, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fig_optimizacion():
    filas = [f for f in leer("optimizacion.csv") if f["aceleracion_x"]]
    etiquetas = {"O1": "O1 · Parquet particionado\nvs. CSV crudo",
                 "O2": "O2 · Expresiones de limpieza\noptimizadas",
                 "O3": "O3 · Broadcast join\nvs. sort-merge join"}
    nombres = [etiquetas[f["optimizacion"]] for f in filas][::-1]
    valores = [float(f["aceleracion_x"]) for f in filas][::-1]
    fig, ax = plt.subplots(figsize=(6.3, 2.4), dpi=220)
    barras = ax.barh(nombres, valores, color=AZUL, height=0.45)
    ax.axvline(1, color=TINTA_2, lw=1, ls="--")
    ax.text(1.05, 2.42, "sin mejora (1×)", color=TINTA_2, fontsize=7.5, va="bottom")
    for b, v in zip(barras, valores):
        ax.text(v + 0.12, b.get_y() + b.get_height() / 2, f"{v:.1f}×".replace(".", ","),
                va="center", color=TINTA, fontsize=9, fontweight="bold")
    ax.set_xlabel("Aceleración = mediana antes ÷ mediana después (7 repeticiones)")
    ax.set_xlim(0, max(valores) * 1.18)
    ax.grid(axis="x", color=GRILLA, lw=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(axis="y", length=0, labelcolor=TINTA)
    fig.tight_layout()
    fig.savefig(OUT / "fig_optimizacion.png")
    plt.close(fig)


def fig_escalabilidad():
    filas = [f for f in leer("escalabilidad.csv") if f["estado"] == "OK"]
    k = [int(f["factor"]) for f in filas]
    t = [float(f["seg_total_proceso"]) for f in filas]
    proporcional = [t[0] * x for x in k]
    fig, ax = plt.subplots(figsize=(6.3, 3.0), dpi=220)
    ax.plot(k, proporcional, color=TINTA_2, lw=1.2, ls="--")
    ax.plot(k, t, color=AZUL, lw=2, marker="o", markersize=6, markeredgecolor="white", markeredgewidth=1.5)
    for x, y, f in zip(k, t, filas):
        reg = int(f["registros_leidos"])
        txt = f"{y:,.0f} s".replace(",", " ") + f" · {reg/1e6:.1f} M filas".replace(".", ",")
        ax.annotate(txt, (x, y), textcoords="offset points", xytext=(6, -16) if x < k[-1] else (-8, -16),
                    color=TINTA, fontsize=8, ha="left" if x < k[-1] else "right")
    ax.text(0.8, 2600, "- - -  línea punteada: crecimiento proporcional al volumen",
            color=TINTA_2, fontsize=7.5, ha="left", va="top")
    ax.text(0.8, 1750, f"        (×100 tardaría {proporcional[-1]:,.0f} s)".replace(",", " "),
            color=TINTA_2, fontsize=7.5, ha="left", va="top")
    ax.text(30, 200, "medido", color=AZUL, fontsize=8.5, fontweight="bold", ha="left")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(k, [f"×{x}" for x in k])
    ax.set_xlim(0.7, 160)
    ax.set_ylim(12, 4000)
    ax.set_ylabel("Tiempo total del pipeline (s, escala log)")
    ax.set_xlabel("Factor de réplica del conjunto real")
    ax.grid(color=GRILLA, lw=0.8, which="major")
    ax.set_axisbelow(True)
    fig.tight_layout()
    fig.savefig(OUT / "fig_escalabilidad.png")
    plt.close(fig)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    fig_optimizacion()
    fig_escalabilidad()
    print("Figuras en", OUT)
