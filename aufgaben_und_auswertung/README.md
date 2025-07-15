# Dokumentation der Rechen­pipeline

Die logistischen Analysen wurden vollständig mit **Python 3.12** durchgeführt.  
Als Datenbasis dient allein die Excel-Datei **`Aufgaben.xlsx`** (Sheet `variants_raw`).

---

## Aufbereitung des Rohblatts  
**`build_variants_sortiert.py`**  
*Imports `pandas`, `re`, `pathlib`*

* erkennt jede Aufgabenzeile (I1 … I29)  
* ordnet ihr exakt die fünf unmittelbar folgenden Varianten zu  
* vererbt die 0/1-Bausteinmarker auf alle Varianten und bildet die Summe `n_bausteine`

> **Output:** `variants_sortiert.csv` · 145 Zeilen (29 Aufgaben × 5 Varianten)

---

## Logistische Regressionen  
**`analyse_bausteine.py`**  
*Imports `pandas`, `numpy`, `statsmodels.api`*

| Modell | Prädiktor | Zielgrößen |
|--------|-----------|------------|
| A | Anzahl der Bausteine | Kompilierung (`comp`), Testerfolg (`pass`) |
| B | Einzelner Baustein (B1 … B12) | Kompilierung (`comp`), Testerfolg (`pass`) |

Cluster-robuste SE (Cluster = Aufgabe).

> **Outputs:**  
> `reg_sum.csv`  ·  β, SE, 95 %-CI, OR, p  
> `reg_presence.csv` ·  pro Baustein × Outcome die gleichen Kennzahlen

---

## Visualisierung  
**`plot_forest_bausteine.py`**  
*Imports `pandas`, `numpy`, `matplotlib`*

Lädt nur `reg_presence.csv` und erzeugt zwei Forest-Plots (Excel-Blau, log-Skala, CI-Balken mit Endstrichen):

| Datei | Titel |
|-------|-------|
| `forest_compilation.png` | Einfluss der Integrationsbausteine auf die erfolgreiche Kompilierung |
| `forest_testerfolg.png` | Einfluss der Integrationsbausteine auf den Testerfolg |

---

*Die Mittelwert-, Standardfehler- und Wilson-Intervall-Analyse für **pass@k** und **compilation@k** wurde separat in Excel 365 berechnet und dort visualisiert.*

Alle drei Python-Skripte laufen hintereinander ohne manuelle Eingriffe; sämtliche Tabellen und Abbildungen in der Arbeit stammen direkt aus den erzeugten CSV- bzw. PNG-Dateien und sind damit vollständig reproduzierbar.

---

## Ausführungsschritte im Terminal

1. **Optionale virtuelle Umgebung anlegen**  
   # Bash:
   python -m venv venv
   # Windows:
   .\venv\Scripts\activate
   # macOS/Linux:
   source venv/bin/activate

2. **Benötigte Pakete installieren**
    pip install pandas numpy matplotlib statsmodels openpyxl

3. **Rohdaten aufbereiten**
    python build_variants_sortiert.py
    # → erzeugt variants_sortiert.csv

4. **Logistische Regression berechnen**
    python analyse_bausteine.py
    # → erzeugt reg_sum.csv und reg_presence.csv

5. **Forest Plot erstellen**
    python plot_forest_bausteine.py
    # → erzeugt forest_compilation.png und forest_testerfolg.png

6. **Ergebnisse öffnen**
