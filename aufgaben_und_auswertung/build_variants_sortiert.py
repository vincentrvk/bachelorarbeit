#!/usr/bin/env python3
# ===============================================================
#  variants_raw  ➜  variants_sortiert.csv
# ---------------------------------------------------------------
# • Liest das Arbeitsblatt "variants_raw" einer Excel-Datei ein
# • Bildet einen sauberen Variant-Level-Datensatz:
#     - variant   : eindeutiger Varianten-Name (I1a … I29e)
#     - cluster   : Aufgaben-ID (z. B. I1)  →  Cluster-Variable
#     - n_bausteine : Summe aller Bausteine dieser Aufgabe
#     - comp / pass : 0/1-Erfolg pro Variante
#     - B1 … Bk   : 0/1, ob Baustein in der Aufgabe vorkommt
# • Speichert das Ergebnis als CSV  (Semikolon / Komma)
# ===============================================================
import re
import sys
import pandas as pd

# ---------------------------------------------------------------
# 1. I/O-Parameter
# ---------------------------------------------------------------
excel_path   = "Aufgaben.xlsx"          # Quelle
output_csv   = "variants_sortiert.csv"  # Ergebnis
raw_sheet    = "variants_raw"           # Blattname
dec_sep      = ","                      # Komma als Dezimaltrennzeichen
field_sep    = ";"                      # Semikolon als Feldtrenner

# ---------------------------------------------------------------
# 2. Rohdaten lesen und Header korrigieren
# ---------------------------------------------------------------
# • Die dritte Excel-Zeile (Index 2, header=2) enthält die Bezeichnungen
raw = pd.read_excel(excel_path, sheet_name=raw_sheet, header=2)

# Zeile direkt darunter enthält die Baustein-Labels → Header ergänzen
header_labels = raw.iloc[0].astype(str).str.strip()
raw.columns   = ["variant"] + header_labels.iloc[1:].tolist()

df = raw.iloc[1:].reset_index(drop=True)        # echte Daten
df["variant_clean"] = df["variant"].astype(str).str.strip()

# ---------------------------------------------------------------
# 3. Aufgaben-Zeilen und Baustein-Spalten ermitteln
# ---------------------------------------------------------------
task_mask   = df["variant_clean"].str.match(r"^[A-Za-z]+\d+$")
tasks_df    = df[task_mask].copy()

baustein_cols = [c for c in df.columns if re.match(r"^B\d+$", str(c))]

# Baustein-Spalten in Integer 0/1 umwandeln
tasks_df[baustein_cols] = (
    tasks_df[baustein_cols]
    .apply(pd.to_numeric, errors="coerce")
    .fillna(0)
    .astype(int)
)

# Summe der Bausteine pro Aufgabe
tasks_df["n_bausteine"] = tasks_df[baustein_cols].sum(axis=1)

# ---------------------------------------------------------------
# 4. Variant-Level-Datensatz bauen
# ---------------------------------------------------------------
variant_rows = []
for task_idx, task_row in tasks_df.iterrows():
    cluster      = task_row["variant_clean"]          # z. B. I17
    n_bausteine  = task_row["n_bausteine"]

    # Die nächsten fünf Zeilen gelten als Varianten
    for offset in range(1, 6):
        var_idx = task_idx + offset
        if var_idx >= len(df):
            continue
        var_row   = df.loc[var_idx]
        var_label = var_row["variant_clean"]

        # Falls Variant-Name fehlt/unklar → synthetisch erzeugen
        if not re.match(r"^[A-Za-z]+\d+[a-eA-E]$", str(var_label)):
            var_label = f"{cluster}{chr(64+offset)}"   # I17A, I17B …

        record = {
            "variant"      : var_label,
            "cluster"      : cluster,
            "n_bausteine"  : n_bausteine,
            "comp"         : int(pd.to_numeric(var_row["comp."], errors="coerce") or 0),
            "pass"         : int(pd.to_numeric(var_row["pass"],   errors="coerce") or 0),
        }

        # Baustein-Präsenz → 0/1 aus Aufgaben-Header erben
        for col in baustein_cols:
            record[col] = task_row[col]

        variant_rows.append(record)

variants_sortiert = pd.DataFrame(variant_rows)

# Prüfen, ob es tatsächlich 5 Varianten je Cluster gibt
check = variants_sortiert.groupby("cluster").size()
if not check.eq(5).all():
    sys.stderr.write("Warnung: Nicht jedes Cluster hat 5 Varianten!\n")

# ---------------------------------------------------------------
# 5. CSV exportieren (Semikolon & Komma)
# ---------------------------------------------------------------
variants_sortiert.to_csv(
    output_csv,
    sep   = field_sep,
    decimal = dec_sep,
    index = False
)

print(f"✓ Fertig – Datei '{output_csv}' geschrieben ({len(variants_sortiert)} Varianten).")
