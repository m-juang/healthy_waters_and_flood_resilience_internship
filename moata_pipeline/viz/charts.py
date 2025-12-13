from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd

# ✅ FIXED: Import dari common/file_utils.py (tidak duplikasi lagi!)
from moata_pipeline.common.file_utils import ensure_dir

logger = logging.getLogger(__name__)


# -------------------------
# Helpers
# -------------------------
# ❌ REMOVED: def ensure_dir() - sudah diimport dari common

def save_fig(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def parse_threshold_num(x: object) -> Optional[float]:
    """
    Parses numeric threshold from a string (tolerant):
    - 180
    - 0.99
    - "180 mm"
    - ">= 15"
    Returns None if cannot parse.
    """
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def normalize_is_critical(series: pd.Series) -> pd.Series:
    """
    Converts is_critical column to boolean (tolerant to:
    True/False, "True"/"False", 1/0, NaN).
    """
    s = series.copy()
    # handle already-bool
    if s.dtype == bool:
        return s.fillna(False)

    # convert string-ish
    s = s.astype(str).str.strip().str.lower()
    return s.isin(["true", "1", "yes", "y"])


# -------------------------
# Public entry
# -------------------------
def build_charts(df: pd.DataFrame, out_dir: Path, max_gauges_for_bars: int = 25, top_gauges_for_ladders: int = 8) -> None:
    ensure_dir(out_dir)

    # Make sure required columns exist
    required = {"gauge_name", "trace_name", "alarm_type", "threshold"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"Missing required columns for charts: {missing}")

    # Add threshold_num safely (does NOT modify original df outside)
    tmp = df.copy()
    tmp["alarm_type_norm"] = tmp["alarm_type"].astype(str).str.strip()
    tmp["threshold_num"] = tmp["threshold"].map(parse_threshold_num)

    # -------------------------
    # 1) Records per gauge
    # -------------------------
    counts_by_gauge = tmp.groupby("gauge_name").size().sort_values(ascending=False)
    top = counts_by_gauge.head(min(max_gauges_for_bars, len(counts_by_gauge)))

    fig, ax = plt.subplots(figsize=(12, 6))
    top.plot(kind="bar", ax=ax)
    ax.set_title(f"Top {len(top)} gauges by number of records in alarm_summary.csv")
    ax.set_xlabel("Gauge")
    ax.set_ylabel("Number of records")
    ax.tick_params(axis="x", rotation=75)
    save_fig(fig, out_dir / "01_records_by_gauge.png")

    # -------------------------
    # 2) Alarm type distribution (replaces row_category)
    # -------------------------
    cat_counts = tmp["alarm_type_norm"].value_counts()

    fig, ax = plt.subplots(figsize=(9, 5))
    cat_counts.plot(kind="bar", ax=ax)
    ax.set_title("Alarm type distribution")
    ax.set_xlabel("alarm_type")
    ax.set_ylabel("Count")
    ax.tick_params(axis="x", rotation=25)
    save_fig(fig, out_dir / "02_record_categories.png")

    # -------------------------
    # 3) Severity distribution (if any)
    # -------------------------
    if "severity" in tmp.columns:
        sev = tmp["severity"].astype(str).str.strip()
        sev = sev[sev.ne("") & sev.ne("nan")]
        sev_counts = sev.value_counts()
        if not sev_counts.empty:
            fig, ax = plt.subplots(figsize=(8, 5))
            sev_counts.plot(kind="bar", ax=ax)
            ax.set_title("Severity distribution (where provided)")
            ax.set_xlabel("severity")
            ax.set_ylabel("Count")
            ax.tick_params(axis="x", rotation=0)
            save_fig(fig, out_dir / "03_severity_distribution.png")

    # -------------------------
    # 4) Threshold histogram (Overflow only)
    # -------------------------
    overflow = tmp[tmp["alarm_type_norm"].str.contains("overflow", case=False, na=False)].copy()
    th = overflow["threshold_num"].dropna()
    if not th.empty:
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.hist(th.values, bins=30)
        ax.set_title("Distribution of numeric threshold values (Overflow alarms)")
        ax.set_xlabel("Threshold value (numeric)")
        ax.set_ylabel("Count")
        save_fig(fig, out_dir / "04_threshold_hist.png")

    # -------------------------
    # 5) Critical flag (if present)
    # -------------------------
    if "is_critical" in tmp.columns:
        crit_bool = normalize_is_critical(tmp["is_critical"])
        crit_counts = crit_bool.value_counts().sort_index()

        fig, ax = plt.subplots(figsize=(6, 5))
        crit_counts.plot(kind="bar", ax=ax)
        ax.set_title("Critical flag count")
        ax.set_xlabel("is_critical")
        ax.set_ylabel("Count")
        ax.tick_params(axis="x", rotation=0)
        save_fig(fig, out_dir / "05_critical_flag.png")

    # -------------------------
    # 6) Threshold ladder charts (THIS is the key)
    # -------------------------
    plot_threshold_ladders(tmp, out_dir, top_gauges=top_gauges_for_ladders)


def plot_threshold_ladders(df: pd.DataFrame, out_dir: Path, top_gauges: int = 8) -> None:
    """
    For each top gauge (by number of overflow threshold rows),
    plots min..max threshold per trace as horizontal segments.
    
    NOTE: Expects df to already have 'threshold_num' column from build_charts().
    """

    ensure_dir(out_dir)

    # Use Overflow rows only
    ladd = df[df["alarm_type_norm"].str.contains("overflow", case=False, na=False)].copy()
    ladd = ladd.dropna(subset=["gauge_name", "trace_name"])

    # ✅ OPTIMIZED: Reuse threshold_num if it exists (already parsed in build_charts)
    # Only parse if missing (defensive programming)
    if "threshold_num" not in ladd.columns:
        logger.warning("threshold_num column missing, parsing from threshold column")
        ladd["threshold_num"] = ladd["threshold"].map(parse_threshold_num)
    
    # Ensure it's numeric (in case it came from outside build_charts)
    ladd["threshold_num"] = pd.to_numeric(ladd["threshold_num"], errors="coerce")

    logger.info(
        "LADDER input: rows=%d, gauges=%d, traces=%d, numeric_thresholds=%d",
        len(ladd),
        ladd["gauge_name"].nunique() if not ladd.empty else 0,
        ladd["trace_name"].nunique() if not ladd.empty else 0,
        int(ladd["threshold_num"].notna().sum()) if not ladd.empty else 0,
    )

    ladd = ladd.dropna(subset=["threshold_num"])
    if ladd.empty:
        logger.warning("LADDER: no rows after threshold_num dropna. Nothing to plot.")
        return

    # Select top gauges by number of overflow rows
    top_list = (
        ladd.groupby("gauge_name")
        .size()
        .sort_values(ascending=False)
        .head(top_gauges)
        .index.tolist()
    )

    for i, gname in enumerate(top_list, start=1):
        gdf = ladd[ladd["gauge_name"] == gname].copy()
        if gdf.empty:
            continue

        agg = (
            gdf.groupby("trace_name")["threshold_num"]
            .agg(["min", "max", "count"])
            .reset_index()
            .sort_values(by=["max", "min"], ascending=[False, False])
        )

        if agg.empty:
            logger.info("LADDER: gauge=%s has empty aggregation, skipping", gname)
            continue

        # Make figure height scale with number of traces so labels don't squash
        height = max(4.0, min(18.0, 0.35 * len(agg) + 2.0))
        fig, ax = plt.subplots(figsize=(11, height))

        y = list(range(len(agg)))
        mins = agg["min"].tolist()
        maxs = agg["max"].tolist()

        # X range padding
        global_min = float(min(mins))
        global_max = float(max(maxs))
        pad = max(1e-6, (global_max - global_min) * 0.08) if global_max != global_min else max(1.0, abs(global_min) * 0.1)
        ax.set_xlim(global_min - pad, global_max + pad)

        # Draw segments / points
        for yi, (mn, mx) in enumerate(zip(mins, maxs)):
            if mn == mx:
                ax.plot([mn], [yi], marker="o")
            else:
                ax.hlines(y=yi, xmin=mn, xmax=mx, linewidth=3)
                ax.plot([mn, mx], [yi, yi], marker="o", linewidth=0)

        ax.set_yticks(y)
        ax.set_yticklabels(agg["trace_name"].tolist())
        ax.set_xlabel("Threshold (numeric)")
        ax.set_title(f"Threshold ladder: {gname}")

        out_path = out_dir / f"06_ladder_gauge_{i}.png"
        save_fig(fig, out_path)

        logger.info("LADDER saved: %s (rows=%d traces=%d)", out_path, len(gdf), len(agg))