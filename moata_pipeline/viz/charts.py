from __future__ import annotations

from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_fig(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()


def build_charts(df: pd.DataFrame, out_dir: Path, max_gauges_for_bars: int = 25) -> None:
    # 1) Records per gauge
    counts_by_gauge = df.groupby("gauge_name").size().sort_values(ascending=False)
    top = counts_by_gauge.head(max_gauges_for_bars)

    plt.figure(figsize=(12, 6))
    top.plot(kind="bar")
    plt.title(f"Top {len(top)} gauges by number of records in alarm_summary.csv")
    plt.xlabel("Gauge")
    plt.ylabel("Number of records")
    plt.xticks(rotation=75, ha="right")
    save_fig(out_dir / "01_records_by_gauge.png")

    # 2) Record categories
    cat_counts = df["row_category"].value_counts()

    plt.figure(figsize=(9, 5))
    cat_counts.plot(kind="bar")
    plt.title("What kinds of records exist in this file?")
    plt.xlabel("Record category")
    plt.ylabel("Count")
    plt.xticks(rotation=25, ha="right")
    save_fig(out_dir / "02_record_categories.png")

    # 3) Severity distribution
    sev = df[df["severity"] != ""].groupby("severity").size().sort_values(ascending=False)
    if not sev.empty:
        plt.figure(figsize=(8, 5))
        sev.plot(kind="bar")
        plt.title("Severity distribution (where provided)")
        plt.xlabel("Severity")
        plt.ylabel("Count")
        plt.xticks(rotation=0)
        save_fig(out_dir / "03_severity_distribution.png")

    # 4) Threshold histogram
    th = df[df["threshold_num"].notna()].copy()
    if not th.empty:
        plt.figure(figsize=(9, 5))
        plt.hist(th["threshold_num"].values, bins=30)
        plt.title("Distribution of numeric threshold values (all traces)")
        plt.xlabel("Threshold value (units depend on trace)")
        plt.ylabel("Count")
        save_fig(out_dir / "04_threshold_hist.png")

    # 5) Critical flag
    crit_counts = df["is_critical_bool"].value_counts().sort_index()
    plt.figure(figsize=(6, 5))
    crit_counts.plot(kind="bar")
    plt.title("Critical flag count")
    plt.xlabel("is_critical")
    plt.ylabel("Count")
    plt.xticks(rotation=0)
    save_fig(out_dir / "05_critical_flag.png")

    # 6) Threshold ladder per top gauges
    top_gauges = counts_by_gauge.head(min(8, len(counts_by_gauge))).index.tolist()
    for i, gname in enumerate(top_gauges, start=1):
        gdf = df[(df["gauge_name"] == gname) & (df["threshold_num"].notna())].copy()
        if gdf.empty:
            continue

        agg = (
            gdf.groupby("trace_name")["threshold_num"]
            .agg(["min", "max", "count"])
            .sort_values(by=["min", "max"], ascending=True)
        )

        plt.figure(figsize=(11, 5))
        y = range(len(agg))
        plt.hlines(y=y, xmin=agg["min"], xmax=agg["max"])
        plt.yticks(ticks=list(y), labels=agg.index)
        plt.title(f"Threshold ladder for gauge: {gname}")
        plt.xlabel("Threshold value (numeric)")
        plt.ylabel("Trace")
        save_fig(out_dir / f"06_ladder_gauge_{i}.png")

def build_risk_chart(df: pd.DataFrame, out_dir: Path, top_n: int = 15) -> None:
    # Define components
    is_crit = df["is_critical_bool"] == True
    is_threshold = df["row_category"] == "Threshold alarm (overflow)"
    is_recency = df["row_category"] == "Data freshness (recency)"

    # Score per row
    # You can tune weights later without touching report/page code
    score = (
        is_crit.astype(int) * 3
        + is_threshold.astype(int) * 2
        + is_recency.astype(int) * 1
    )

    tmp = df.copy()
    tmp["risk_points"] = score

    by_gauge = tmp.groupby("gauge_name")["risk_points"].sum().sort_values(ascending=False)
    top = by_gauge.head(top_n)

    if top.empty:
        return

    plt.figure(figsize=(12, 6))
    top.plot(kind="bar")
    plt.title(f"Top {len(top)} gauges by risk score (critical + thresholds + recency)")
    plt.xlabel("Gauge")
    plt.ylabel("Risk score (weighted)")
    plt.xticks(rotation=75, ha="right")
    save_fig(out_dir / "08_top_risky_gauges.png")

    # 8) Risk chart
    build_risk_chart(df, out_dir, top_n=15)
