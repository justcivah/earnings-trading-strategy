# plot_results.py
# Produce diagnostic plots from aggregated_results.csv
# Requirements: pandas, matplotlib, seaborn
# Usage: python plot_results.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid", context="talk")

INPUT_CSV = "aggregated_results.csv"
OUT_DIR = "figures"
os.makedirs(OUT_DIR, exist_ok=True)

df = pd.read_csv(INPUT_CSV)

# Ensure boolean column is proper dtype
if df["equal_investment"].dtype == object:
    df["equal_investment"] = df["equal_investment"].map(
        {"True": True, "False": False}
    )

# Ensure numeric types
numeric_cols = [
    "score_threshold",
    "technical_weight",
    "sentiment_weight",
    "min_market_cap",
    "max_market_cap",
    "total_operations",
    "roi",
    "avg_profit_per_op",
    "win_rate",
]
for c in numeric_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# 1) Histogram of ROI (with KDE, counts, show median & mean)
plt.figure(figsize=(8, 5))
sns.histplot(df["roi"].dropna(), bins=20, kde=True, color="#2b8cbe", stat="count")
plt.axvline(df["roi"].mean(), color="darkred", linestyle="--", linewidth=2, label="Mean")
plt.xlabel("ROI")
plt.ylabel("Parameter combinations")
plt.title("ROI distribution across parameter combinations")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, "roi_histogram.png"), dpi=200)
plt.close()

# 2) ROI vs Score Threshold, grouped by equal_investment
grouped = (
    df.groupby(["score_threshold", "equal_investment"])["roi"]
    .agg(["mean", "count", "std"])
    .reset_index()
)
grouped["sem"] = grouped["std"] / grouped["count"] ** 0.5
plt.figure(figsize=(9, 6))
for invest_flag, grp in grouped.groupby("equal_investment"):
    plt.plot(
        grp["score_threshold"],
        grp["mean"],
        marker="o",
        label=f"Equal investment={invest_flag}",
    )
    plt.fill_between(
        grp["score_threshold"],
        grp["mean"] - grp["sem"],
        grp["mean"] + grp["sem"],
        alpha=0.2,
    )
plt.xlabel("Score threshold")
plt.ylabel("Mean ROI")
plt.title("Mean ROI vs Score Threshold")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, "roi_vs_score_threshold.png"), dpi=200)
plt.close()

# 3) Boxplot: ROI grouped by technical_weight
plt.figure(figsize=(9, 6))
order = sorted(df["technical_weight"].dropna().unique())
sns.boxplot(x="technical_weight", y="roi", data=df, order=order, palette="vlag")
plt.xlabel("Technical weight")
plt.ylabel("ROI")
plt.title("ROI distribution by Technical weight")
plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, "roi_by_technical_weight.png"), dpi=200)
plt.close()

# 4) Scatter + trend: win_rate vs avg_profit_per_op
plt.figure(figsize=(9, 6))
palette = {True: "#2b8cbe", False: "#d95f02"}
fixed_marker_size = 90

# Raw points
ax = sns.scatterplot(
    data=df,
    x="win_rate",
    y="avg_profit_per_op",
    hue="equal_investment",
    palette=palette,
    s=fixed_marker_size,
    alpha=0.85,
    edgecolor="k",
    linewidth=0.4,
)

# Trend line (ignores hue for clarity)
sns.regplot(
    x="win_rate",
    y="avg_profit_per_op",
    data=df,
    scatter=False,
    color="black",
    line_kws={"linewidth": 2, "alpha": 0.8},
)

plt.xlabel("Win rate (%)")
plt.ylabel("Average profit per operation")
plt.title("Win rate vs Average profit per operation (with trend)")

# Make legend smaller
leg = ax.legend(title="Equal investment", loc="best")
plt.setp(leg.get_title(), fontsize="small")
for text in leg.get_texts():
    text.set_fontsize("small")

plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, "winrate_vs_avgprofit.png"), dpi=200)
plt.close()


print("Plots saved to:", OUT_DIR)