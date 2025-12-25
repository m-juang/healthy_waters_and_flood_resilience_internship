from __future__ import annotations

import html
from pathlib import Path

import pandas as pd

from moata_pipeline.common.text_utils import safe_filename
from moata_pipeline.common.file_utils import ensure_dir
from moata_pipeline.common.html_utils import df_to_html_table


def build_gauge_pages(df: pd.DataFrame, out_dir: Path) -> None:
    pages_dir = out_dir / "gauge_pages"
    ensure_dir(pages_dir)

    gauges = sorted([g for g in df["Gauge"].unique() if str(g).strip() != ""])
    for gname in gauges:
        gdf = df[df["Gauge"] == gname].copy()

        # === OVERFLOW TABLE ===
        overflow = gdf[gdf["row_category"] == "Threshold alarm (overflow)"].copy()
        overflow_table = (
            overflow[["Trace", "Alarm Name", "Threshold"]]
            .drop_duplicates()
            .sort_values(by=["Trace", "Threshold"], ascending=[True, True], na_position="last")
        )

        # === RECENCY TABLE ===
        recency = gdf[gdf["row_category"] == "Data freshness (recency)"].copy()
        recency_table = (
            recency[["Trace", "Alarm Name", "Threshold"]]
            .drop_duplicates()
            .sort_values(by=["Trace"], ascending=True)
            .rename(columns={"Threshold": "Hours Since Last Data"})
        )

        css = """
        <style>
          body { font-family: Arial, sans-serif; margin: 24px; line-height: 1.4; }
          h1 { margin-bottom: 6px; }
          h2 { margin-top: 28px; color: #333; }
          .muted { color: #555; }
          .note { background: #e8f4fd; border-left: 4px solid #2c5282; padding: 12px 16px; margin: 16px 0; border-radius: 4px; }
          table { border-collapse: collapse; width: 100%; margin-top: 12px; }
          th, td { border: 1px solid #ddd; padding: 8px 10px; font-size: 0.95em; }
          th { background: #f3f3f3; text-align: left; }
          tr:nth-child(even) { background: #fafafa; }
          .stats { display: flex; gap: 20px; flex-wrap: wrap; margin: 16px 0; }
          .stat-box { background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 8px; padding: 10px 16px; }
          .stat-box .num { font-size: 1.5em; font-weight: bold; color: #2c5282; }
          .stat-box .label { color: #666; font-size: 0.85em; }
        </style>
        """

        total_overflow = len(overflow_table)
        total_recency = len(recency_table)

        parts = [
            "<html><head><meta charset='utf-8'/>",
            f"<title>{html.escape(gname)}</title>",
            css,
            "</head><body>",
            f"<h1>{html.escape(gname)}</h1>",

            # Stats
            "<div class='stats'>",
            f"<div class='stat-box'><div class='num'>{total_overflow}</div><div class='label'>Overflow Alarms</div></div>",
            f"<div class='stat-box'><div class='num'>{total_recency}</div><div class='label'>Recency Monitors</div></div>",
            "</div>",

            # Overflow table
            "<h2>Overflow/Threshold Alarms</h2>",
            df_to_html_table(overflow_table, "", max_rows=500),

            # Recency table
            "<h2>Data Freshness (Recency)</h2>",
            "<div class='note'>"
            "<b>Note:</b> \"Hours Since Last Data\" shows how long ago this gauge last reported data "
            "when the report was generated."
            "</div>",
            df_to_html_table(recency_table, "", max_rows=100),

            # Back link
            "<hr/>",
            "<p class='muted'><a href='../report.html'>← Back to main report</a></p>",
            "</body></html>",
        ]

        # === WRITE FILE ===
        fname = safe_filename(gname) + ".html"
        (pages_dir / fname).write_text("\n".join(parts), encoding="utf-8")