"""
visualize_alarm_summary_v2.py

Create a non-technical, easy-to-read visual report from alarm_summary.csv.

Input (default):
- moata_filtered/alarm_summary.csv

Expected columns (from your CSV):
  gauge_id,gauge_name,last_data,trace_id,trace_name,
  alarm_id,alarm_name,alarm_type,threshold,severity,is_critical,source

Output (default):
- moata_filtered/viz/
    report.html
    cleaned_alarm_summary.csv
    01_records_by_gauge.png
    02_record_categories.png
    03_severity_distribution.png (if available)
    04_threshold_hist.png        (if numeric thresholds exist)
    05_critical_flag.png
    06_ladder_gauge_*.png        (top gauges threshold ladders)
    07_gauge_pages/
        <safe_gauge_name>.html   (one page per gauge)

Install:
  pip install pandas matplotlib python-dateutil

Run:
  python visualize_alarm_summary_v2.py
  python visualize_alarm_summary_v2.py --csv moata_filtered/alarm_summary.csv --out moata_filtered/viz
"""

from __future__ import annotations

import argparse
import html
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt


# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# -------------------------
# Helpers
# -------------------------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_fig(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()


def parse_date_series(s: pd.Series) -> pd.Series:
    # Your CSV uses ISO dates like 2025-12-12
    return pd.to_datetime(s, errors="coerce")


def to_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def to_bool_series(s: pd.Series) -> pd.Series:
    # Handles True/False, "True"/"False", "TRUE"/"FALSE", 1/0, yes/no
    if s.dtype == bool:
        return s
    mapped = (
        s.astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
    )
    return mapped.fillna(False).astype(bool)


def safe_filename(name: str, max_len: int = 120) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\s\-\.]", "_", name)
    name = re.sub(r"\s+", "_", name)
    return name[:max_len] if len(name) > max_len else name


def df_to_html_table(df: pd.DataFrame, title: str, max_rows: int = 50) -> str:
    if df.empty:
        return f"<h3>{html.escape(title)}</h3><p><em>No rows.</em></p>"
    view = df.head(max_rows).copy()
    return (
        f"<h3>{html.escape(title)}</h3>"
        f"<p><em>Showing first {min(len(view), max_rows)} rows.</em></p>"
        + view.to_html(index=False, escape=True)
    )


def img_block(img_name: str, caption: str) -> str:
    return f"""
    <div class="card">
      <img src="{html.escape(img_name)}" alt="{html.escape(caption)}"/>
      <p class="caption">{html.escape(caption)}</p>
    </div>
    """


# -------------------------
# Load + clean
# -------------------------
def load_and_clean(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]

    # Ensure all expected columns exist (create empty if missing)
    expected = [
        "gauge_id",
        "gauge_name",
        "last_data",
        "trace_id",
        "trace_name",
        "alarm_id",
        "alarm_name",
        "alarm_type",
        "threshold",
        "severity",
        "is_critical",
        "source",
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA

    # Types
    df["gauge_id"] = pd.to_numeric(df["gauge_id"], errors="coerce").astype("Int64")
    df["trace_id"] = pd.to_numeric(df["trace_id"], errors="coerce").astype("Int64")

    df["last_data_dt"] = parse_date_series(df["last_data"])
    df["threshold_num"] = to_numeric_series(df["threshold"])
    df["is_critical_bool"] = to_bool_series(df["is_critical"])

    # Normalize text fields
    for col in ["gauge_name", "trace_name", "alarm_name", "alarm_type", "severity", "source"]:
        df[col] = df[col].astype(str).replace({"nan": ""}).fillna("").str.strip()

    # Categorize rows into something non-technical
    def classify_row(r: pd.Series) -> str:
        src = (r.get("source") or "").lower()
        at = (r.get("alarm_type") or "").lower()
        alarm_id = r.get("alarm_id")
        has_alarm_id = pd.notna(alarm_id) and str(alarm_id).strip() != ""

        if "has_alarms" in src or at == "datarecency":
            return "Data freshness (recency)"
        if "threshold" in src or at == "overflow":
            return "Threshold alarm (overflow)"
        if has_alarm_id:
            return "Alarm record"
        return "Other"

    df["row_category"] = df.apply(classify_row, axis=1)

    # Nice sort
    df = df.sort_values(
        by=["gauge_name", "trace_name", "row_category", "threshold_num"],
        ascending=[True, True, True, True],
        na_position="last",
    )

    return df


# -------------------------
# Charts
# -------------------------
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


# -------------------------
# Gauge pages (1 page per gauge)
# -------------------------
def build_gauge_pages(df: pd.DataFrame, out_dir: Path) -> None:
    pages_dir = out_dir / "07_gauge_pages"
    ensure_dir(pages_dir)

    gauges = sorted([g for g in df["gauge_name"].unique() if str(g).strip() != ""])
    for gname in gauges:
        gdf = df[df["gauge_name"] == gname].copy()

        latest = gdf["last_data_dt"].max()
        oldest = gdf["last_data_dt"].min()

        overflow = gdf[gdf["row_category"] == "Threshold alarm (overflow)"].copy()
        recency = gdf[gdf["row_category"] == "Data freshness (recency)"].copy()
        crit = gdf[gdf["is_critical_bool"]].copy()

        # Sorted overflow table (this is where your previous error came from)
        overflow_table = (
            overflow[
                [
                    "trace_name",
                    "alarm_name",
                    "threshold",
                    "threshold_num",
                    "severity",
                    "is_critical",
                    "last_data",
                    "source",
                ]
            ]
            .sort_values(by=["trace_name", "threshold_num"], ascending=[True, True], na_position="last")
            .drop(columns=["threshold_num"])
        )

        recency_table = (
            recency[["trace_name", "alarm_type", "alarm_name", "last_data", "source"]]
            .drop_duplicates()
            .sort_values(by=["trace_name"], ascending=True)
        )

        crit_table = (
            crit[
                ["trace_name", "alarm_type", "alarm_name", "threshold", "severity", "last_data", "source"]
            ]
            .sort_values(by=["severity", "trace_name"], ascending=[False, True], na_position="last")
        )

        def dt_fmt(x: Optional[pd.Timestamp]) -> str:
            if pd.isna(x) or x is None:
                return "Unknown"
            return x.strftime("%Y-%m-%d")

        css = """
        <style>
          body { font-family: Arial, sans-serif; margin: 24px; line-height: 1.35; }
          h1 { margin-bottom: 6px; }
          .muted { color: #555; }
          .note { background: #fff7e6; border: 1px solid #ffe0a3; padding: 10px 12px; border-radius: 10px; }
          table { border-collapse: collapse; width: 100%; }
          th, td { border: 1px solid #ddd; padding: 6px 8px; font-size: 0.95em; }
          th { background: #f3f3f3; text-align: left; }
          code { background: #f5f5f5; padding: 1px 5px; border-radius: 6px; }
        </style>
        """

        parts = [
            "<html><head><meta charset='utf-8'/>",
            f"<title>{html.escape(gname)} - Alarm Summary</title>",
            css,
            "</head><body>",
            f"<h1>{html.escape(gname)}</h1>",
            f"<p class='muted'>Date range in file for this gauge: <b>{dt_fmt(oldest)}</b> to <b>{dt_fmt(latest)}</b></p>",
            "<div class='note'>"
            "<b>Interpretation:</b><br/>"
            "• <b>Threshold alarm (overflow)</b> = trigger levels for rainfall windows (e.g., 15mm in 30min).<br/>"
            "• <b>Data freshness (recency)</b> = monitoring if the sensor stops updating / data goes stale.<br/>"
            "• <b>Critical</b> = flagged critical in the dataset (higher attention)."
            "</div><br/>",
            df_to_html_table(crit_table, "Critical records", max_rows=200),
            df_to_html_table(overflow_table, "Overflow threshold configurations", max_rows=500),
            df_to_html_table(recency_table, "Recency monitoring flags", max_rows=500),
            "<hr/>",
            f"<p class='muted'>Back to main report: <a href='../report.html'>report.html</a></p>",
            "</body></html>",
        ]

        fname = safe_filename(gname) + ".html"
        (pages_dir / fname).write_text("\n".join(parts), encoding="utf-8")


# -------------------------
# Main report
# -------------------------
def build_report(df: pd.DataFrame, out_dir: Path) -> None:
    total_rows = len(df)
    gauges = df["gauge_name"].nunique()
    traces = df["trace_id"].nunique()

    latest = df["last_data_dt"].max()
    oldest = df["last_data_dt"].min()

    # Tables
    crit = df[df["is_critical_bool"]].copy()
    crit_table = crit[
        ["gauge_name", "trace_name", "alarm_type", "alarm_name", "threshold", "severity", "source", "last_data"]
    ].sort_values(by=["severity", "gauge_name", "trace_name"], ascending=[False, True, True], na_position="last")

    recency = df[df["row_category"] == "Data freshness (recency)"].copy()
    recency_table = (
        recency[["gauge_name", "trace_name", "alarm_type", "alarm_name", "source", "last_data"]]
        .drop_duplicates()
        .sort_values(by=["gauge_name", "trace_name"], ascending=[True, True])
    )

    overflow = df[df["row_category"] == "Threshold alarm (overflow)"].copy()
    overflow_table = (
        overflow[
            ["gauge_name", "trace_name", "alarm_name", "threshold", "threshold_num", "severity", "is_critical", "last_data"]
        ]
        .sort_values(by=["gauge_name", "trace_name", "threshold_num"], ascending=[True, True, True], na_position="last")
        .drop(columns=["threshold_num"])
    )

    # Gauge page links
    gauges_list = sorted([g for g in df["gauge_name"].unique() if str(g).strip() != ""])
    link_rows = []
    for g in gauges_list:
        link = f"07_gauge_pages/{safe_filename(g)}.html"
        link_rows.append({"Gauge": g, "Open": f"<a href='{html.escape(link)}'>View</a>"})
    links_df = pd.DataFrame(link_rows)

    css = """
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; line-height: 1.35; }
      h1 { margin-bottom: 6px; }
      .muted { color: #555; }
      .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }
      .card { border: 1px solid #ddd; border-radius: 10px; padding: 12px; background: #fff; }
      .card img { width: 100%; height: auto; border-radius: 8px; border: 1px solid #eee; }
      .caption { margin: 8px 0 0 0; color: #444; font-size: 0.95em; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #ddd; padding: 6px 8px; font-size: 0.95em; }
      th { background: #f3f3f3; text-align: left; }
      .note { background: #fff7e6; border: 1px solid #ffe0a3; padding: 10px 12px; border-radius: 10px; }
      code { background: #f5f5f5; padding: 1px 5px; border-radius: 6px; }
      .small { font-size: 0.95em; }
    </style>
    """

    def dt_fmt(x: Optional[pd.Timestamp]) -> str:
        if pd.isna(x) or x is None:
            return "Unknown"
        return x.strftime("%Y-%m-%d")

    html_parts = [
        "<html><head><meta charset='utf-8'/>",
        "<title>Alarm Summary Report</title>",
        css,
        "</head><body>",
        "<h1>Alarm Summary Report (Non-technical view)</h1>",
        f"<p class='muted'>Generated from <code>alarm_summary.csv</code>. "
        f"Rows: <b>{total_rows}</b> · Gauges: <b>{gauges}</b> · Traces: <b>{traces}</b> · "
        f"Date range in file: <b>{dt_fmt(oldest)}</b> to <b>{dt_fmt(latest)}</b></p>",

        "<div class='note'>"
        "<b>How to read this report:</b><br/>"
        "• <b>Threshold alarm (overflow)</b> rows show trigger levels (e.g., 15mm in 30 min).<br/>"
        "• <b>Data freshness (recency)</b> rows indicate monitoring for missing/stale data (sensor not updating).<br/>"
        "• <b>Critical</b> = flagged as critical in the dataset (higher attention)."
        "</div><br/>",

        "<h2>Quick navigation: one page per gauge</h2>",
        "<p class='muted small'>Click <b>View</b> to open a gauge-specific page with all thresholds and recency flags.</p>",
        links_df.to_html(index=False, escape=False),

        "<h2>Visual overview</h2>",
        "<div class='grid'>",
        img_block("01_records_by_gauge.png", "Which gauges have the most records? (Top gauges)"),
        img_block("02_record_categories.png", "Record categories: threshold vs data freshness vs others"),
        img_block("05_critical_flag.png", "How many records are marked critical vs not"),
        "</div>",
        "<div class='grid'>",
    ]

    # Conditionally include images if they exist
    for img, cap in [
        ("03_severity_distribution.png", "Severity distribution (if present)"),
        ("04_threshold_hist.png", "Distribution of numeric threshold values"),
    ]:
        if (out_dir / img).exists():
            html_parts.append(img_block(img, cap))
    html_parts.append("</div>")

    html_parts += [
        "<h2>Threshold ladders (most intuitive)</h2>",
        "<p class='muted'>These charts show the trigger levels per trace for the top gauges. "
        "A longer line means a trace has a range of thresholds (min..max).</p>",
        "<div class='grid'>",
    ]

    for i in range(1, 9):
        img = out_dir / f"06_ladder_gauge_{i}.png"
        if img.exists():
            html_parts.append(img_block(img.name, f"Threshold ladder (Gauge #{i})"))
    html_parts.append("</div>")

    html_parts += [
        "<h2>Actionable tables</h2>",
        df_to_html_table(crit_table, "Critical records (if any)", max_rows=200),
        df_to_html_table(overflow_table, "Overflow threshold configurations (sorted)", max_rows=300),
        df_to_html_table(recency_table, "Data freshness monitoring (recency) flags", max_rows=300),

        "<h2>Full dataset preview</h2>",
        df_to_html_table(df.drop(columns=["last_data_dt", "threshold_num"], errors="ignore"), "Raw view (first rows)", max_rows=120),

        "<hr/>",
        "<p class='muted'>Tip: You can send this <code>report.html</code> folder to stakeholders. "
        "They only need a browser to open it.</p>",
        "</body></html>",
    ]

    (out_dir / "report.html").write_text("\n".join(html_parts), encoding="utf-8")


# -------------------------
# Main
# -------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize alarm_summary.csv for non data scientists.")
    parser.add_argument(
        "--csv",
        type=str,
        default=str(Path("moata_filtered") / "alarm_summary.csv"),
        help="Path to alarm_summary.csv",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=str(Path("moata_filtered") / "viz"),
        help="Output folder for report and images",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    ensure_dir(out_dir)

    logging.info("Loading CSV: %s", csv_path)
    df = load_and_clean(csv_path)

    # Save cleaned copy
    cleaned_path = out_dir / "cleaned_alarm_summary.csv"
    df.drop(columns=["last_data_dt", "threshold_num"], errors="ignore").to_csv(cleaned_path, index=False)
    logging.info("Saved cleaned CSV: %s", cleaned_path)

    # Charts
    logging.info("Building charts...")
    build_charts(df, out_dir)

    # Gauge pages
    logging.info("Building per-gauge pages...")
    build_gauge_pages(df, out_dir)

    # Main report
    logging.info("Building main report...")
    build_report(df, out_dir)

    logging.info("DONE. Open in browser: %s", out_dir / "report.html")
    print(f"\n✅ Done! Open in browser: {out_dir / 'report.html'}")


if __name__ == "__main__":
    main()
