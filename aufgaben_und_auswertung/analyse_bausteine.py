# =========================================================
# Logistische Regressionen (cluster-robust) aus variants_sortiert
# und CSV-Export der Ergebnisse
# =========================================================
import re
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tools.sm_exceptions import PerfectSeparationError

# ---------------------------------------------------------
# 0) Ausgangs-DataFrame
# ---------------------------------------------------------
variants_sortiert = pd.read_csv("variants_sortiert.csv", sep=";", decimal=",")

df = variants_sortiert.copy()

# ---------------------------------------------------------
# 1) Setup
# ---------------------------------------------------------
baustein_cols = [c for c in df.columns if re.match(r"^B\d+$", str(c))]
cluster_col   = "cluster"

# ---------------------------------------------------------
# 2) Hilfsfunktion – Logit mit cluster-SEs
# ---------------------------------------------------------
def logit_cluster(data: pd.DataFrame, outcome: str, predictor: str):
    X = sm.add_constant(data[[predictor]])
    model = sm.Logit(data[outcome], X)

    try:
        res    = model.fit(disp=False,
                           cov_type="cluster",
                           cov_kwds={"groups": data[cluster_col]})
        beta   = res.params[predictor]
        se     = res.bse[predictor]
        ci_lo  = beta - 1.96 * se
        ci_hi  = beta + 1.96 * se
        or_    = np.exp(beta)
        or_lo  = np.exp(ci_lo)
        or_hi  = np.exp(ci_hi)
        p_val  = res.pvalues[predictor]
    except (PerfectSeparationError, np.linalg.LinAlgError):
        beta = se = ci_lo = ci_hi = or_ = or_lo = or_hi = p_val = np.nan

    return beta, se, ci_lo, ci_hi, or_, or_lo, or_hi, p_val

# ---------------------------------------------------------
# 3) Modell A – Summe der Bausteine
# ---------------------------------------------------------
sum_rows = []
for outcome in ["comp", "pass"]:
    beta, se, lo, hi, or_, or_lo, or_hi, p = logit_cluster(df, outcome, "n_bausteine")
    sum_rows.append({
        "Outcome": outcome,
        "β": beta,
        "SE": se,
        "CI_low": lo,
        "CI_up": hi,
        "OR": or_,
        "OR_low": or_lo,
        "OR_up": or_hi,
        "p": p
    })
sum_results_df = pd.DataFrame(sum_rows)

# ---------------------------------------------------------
# 4) Modell B – Vorhandensein einzelner Bausteine
# ---------------------------------------------------------
presence_rows = []
for b in baustein_cols:
    for outcome in ["comp", "pass"]:
        beta, se, lo, hi, or_, or_lo, or_hi, p = logit_cluster(df, outcome, b)
        presence_rows.append({
            "Outcome": outcome,
            "Baustein": b,
            "β": beta,
            "SE": se,
            "CI_low": lo,
            "CI_up": hi,
            "OR": or_,
            "OR_low": or_lo,
            "OR_up": or_hi,
            "p": p
        })

presence_results_df = (
    pd.DataFrame(presence_rows)
    .sort_values(["Outcome", "Baustein"])
    .reset_index(drop=True)
)

# ---------------------------------------------------------
# 5) CSV-Export (Semikolon / Komma)
# ---------------------------------------------------------
sum_results_df.to_csv("reg_sum.csv",       sep=";", decimal=",", index=False)
presence_results_df.to_csv("reg_presence.csv", sep=";", decimal=",", index=False)

print("► Ergebnisse gespeichert:")
print("  • reg_sum.csv       – Einfluss der Baustein-Summe")
print("  • reg_presence.csv  – Vorhandensein einzelner Bausteine")
