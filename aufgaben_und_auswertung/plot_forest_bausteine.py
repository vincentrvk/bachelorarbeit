#!/usr/bin/env python3
# =============================================================
# Forest-Plots aus "reg_presence.csv"
# =============================================================
#  • CSV entsteht vorher durch analyse_bausteine.py
#  • Spalten: Outcome;Baustein;β;SE;CI_low;CI_up;OR;OR_low;OR_up;p
#  • Erzeugt zwei PNGs im Excel-Blau (#5B9BD5) mit Caps an CI-Enden
# =============================================================

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ----------------------------------------------------------------
# 1) Dateien & Farben
# ----------------------------------------------------------------
infile  = "reg_presence.csv"          # Semikolon, Komma
out_dir = Path(".")
color   = "#5B9BD5"                   # Excel-Accent-1

# ----------------------------------------------------------------
# 2) Daten einlesen
# ----------------------------------------------------------------
df = pd.read_csv(infile, sep=";", decimal=",")

# Numerische Spalten sicher konvertieren
for c in ["OR", "OR_low", "OR_up"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# ----------------------------------------------------------------
# 3) Plot-Funktion
# ----------------------------------------------------------------
def make_forest(sub: pd.DataFrame, title: str, png_name: str):
    sub = sub.sort_values("OR").reset_index(drop=True)
    y   = np.arange(len(sub))

    fig, ax = plt.subplots(figsize=(8, 6))

    # CI ohne Kappen
    ax.errorbar(
        sub["OR"], y,
        xerr=[sub["OR"] - sub["OR_low"], sub["OR_up"] - sub["OR"]],
        fmt="o", color=color, ecolor=color, capsize=0
    )

    # Vertikale Striche (Kappen)
    cap_h = 0.25
    for yi, lo, up in zip(y, sub["OR_low"], sub["OR_up"]):
        ax.vlines([lo, up], yi - cap_h/2, yi + cap_h/2, color=color, lw=1)

    ax.axvline(1, ls="--", color="grey")
    ax.set_xscale("log")
    ax.set_xlabel("Odds Ratio (log-Skala)")
    ax.set_yticks(y)
    ax.set_yticklabels(sub["Baustein"])
    ax.set_title(title)
    plt.tight_layout()
    fig.savefig(out_dir / png_name, dpi=300)
    plt.close(fig)

# ----------------------------------------------------------------
# 4) Zwei Plots erzeugen
# ----------------------------------------------------------------
make_forest(
    df[df["Outcome"].str.contains("comp", case=False)],
    "Einfluss der Integrationsbausteine auf die erfolgreiche Kompilierung",
    "forest_compilation.png"
)

make_forest(
    df[df["Outcome"].str.contains("pass", case=False)],
    "Einfluss der Integrationsbausteine auf den Testerfolg",
    "forest_testerfolg.png"
)

print("✓ Forest-Plots gespeichert (forest_compilation.png, forest_testerfolg.png)")
