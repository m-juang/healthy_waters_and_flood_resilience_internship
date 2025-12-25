from __future__ import annotations

import html
from pathlib import Path

import pandas as pd

from moata_pipeline.common.text_utils import safe_filename
from moata_pipeline.common.html_utils import df_to_html_table


def build_report(df: pd.DataFrame, out_dir: Path) -> None:
    total_rows = len(df)
    gauges = df["Gauge"].nunique()
    trace_types = df["Trace"].nunique()  # Renamed for clarity

    # === OVERFLOW TABLE ===
    overflow = df[df["row_category"] == "Threshold alarm (overflow)"].copy()
    overflow_table = (
        overflow[["Gauge", "Trace", "Alarm Name", "Threshold"]]
        .drop_duplicates()
        .sort_values(by=["Gauge", "Trace", "Threshold"], ascending=[True, True, True], na_position="last")
    )

    # === RECENCY TABLE ===
    recency = df[df["row_category"] == "Data freshness (recency)"].copy()
    recency_table = (
        recency[["Gauge", "Trace", "Alarm Name", "Threshold"]]
        .drop_duplicates()
        .sort_values(by=["Gauge", "Threshold"], ascending=[True, False], na_position="last")
        .rename(columns={"Threshold": "Hours Since Last Data"})
    )

    # === SUMMARY: Gauges with alarm counts ===
    summary = df.groupby("Gauge").agg(
        traces=("Trace", "nunique"),
        overflow_alarms=("row_category", lambda s: (s == "Threshold alarm (overflow)").sum()),
        recency_alarms=("row_category", lambda s: (s == "Data freshness (recency)").sum()),
    ).reset_index()
    summary.columns = ["Gauge", "Trace Types", "Overflow Alarms", "Recency Monitors"]
    summary = summary.sort_values(by="Gauge")

    # === GAUGE LIST with links ===
    gauges_list = sorted([g for g in df["Gauge"].unique() if str(g).strip() != ""])
    link_rows = []
    for g in gauges_list:
        link = f"gauge_pages/{safe_filename(g)}.html"
        link_rows.append({"Gauge": g, "Open": f"<a href='{html.escape(link)}'>View</a>"})
    links_df = pd.DataFrame(link_rows)

    css = """
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; line-height: 1.4; }
      h1 { margin-bottom: 6px; }
      h2 { margin-top: 32px; color: #333; }
      .muted { color: #555; }
      .note { background: #e8f4fd; border-left: 4px solid #2c5282; padding: 12px 16px; margin: 16px 0; border-radius: 4px; }
      table { border-collapse: collapse; width: 100%; margin-top: 12px; }
      th, td { border: 1px solid #ddd; padding: 8px 10px; font-size: 0.95em; }
      th { background: #f3f3f3; text-align: left; }
      tr:nth-child(even) { background: #fafafa; }
      .stats { display: flex; gap: 24px; flex-wrap: wrap; margin: 16px 0; }
      .stat-box { background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 8px; padding: 12px 20px; }
      .stat-box .num { font-size: 1.8em; font-weight: bold; color: #2c5282; }
      .stat-box .label { color: #666; font-size: 0.9em; }
    </style>
    """

    html_parts = [
        "<html><head><meta charset='utf-8'/>",
        "<title>Rain Gauge Alarm Configuration</title>",
        css,
        "</head><body>",
        "<h1>Rain Gauge Alarm Configuration</h1>",
        "<p class='muted'>Active rain gauges with configured alarms</p>",

        # Stats boxes
        "<div class='stats'>",
        f"<div class='stat-box'><div class='num'>{gauges}</div><div class='label'>Rain Gauges</div></div>",
        f"<div class='stat-box'><div class='num'>{trace_types}</div><div class='label'>Trace Types</div></div>",
        f"<div class='stat-box'><div class='num'>{len(overflow_table)}</div><div class='label'>Overflow Alarms</div></div>",
        f"<div class='stat-box'><div class='num'>{len(recency_table)}</div><div class='label'>Recency Monitors</div></div>",
        "</div>",

        # Summary table
        "<h2>Summary per Gauge</h2>",
        "<p class='muted'>Overview of each gauge with alarm counts.</p>",
        summary.to_html(index=False, escape=True),

        # Overflow alarms
        "<h2>Overflow/Threshold Alarms</h2>",
        "<p class='muted'>Alarms triggered when rainfall exceeds a threshold.</p>",
        df_to_html_table(overflow_table, "", max_rows=1000),

        # Recency monitoring
        "<h2>Data Freshness (Recency) Monitoring</h2>",
        "<div class='note'>"
        "<b>Note:</b> \"Hours Since Last Data\" shows how long ago the gauge last reported data "
        "when this report was generated."
        "</div>",
        df_to_html_table(recency_table, "", max_rows=500),

        # Gauge page links
        "<h2>Individual Gauge Pages</h2>",
        "<p class='muted'>Click to view details for each gauge.</p>",
        links_df.to_html(index=False, escape=False),

        "</body></html>",
    ]

    (out_dir / "report.html").write_text("\n".join(html_parts), encoding="utf-8")