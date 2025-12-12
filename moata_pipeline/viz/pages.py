from __future__ import annotations

import html
from pathlib import Path
from typing import Optional

import pandas as pd

from moata_pipeline.common.text_utils import safe_filename


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def df_to_html_table(df: pd.DataFrame, title: str, max_rows: int = 50) -> str:
    if df.empty:
        return f"<h3>{html.escape(title)}</h3><p><em>No rows.</em></p>"
    view = df.head(max_rows).copy()
    return (
        f"<h3>{html.escape(title)}</h3>"
        f"<p><em>Showing first {min(len(view), max_rows)} rows.</em></p>"
        + view.to_html(index=False, escape=True)
    )


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
            crit[["trace_name", "alarm_type", "alarm_name", "threshold", "severity", "last_data", "source"]]
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
        total_records = len(gdf)
        total_crit = int(gdf["is_critical_bool"].sum())
        total_threshold = int((gdf["row_category"] == "Threshold alarm (overflow)").sum())
        total_recency = int((gdf["row_category"] == "Data freshness (recency)").sum())


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
            "<p class='muted'><b>Quick summary:</b> "
            f"{total_records} records · "
            f"{total_crit} critical · "
            f"{total_threshold} threshold · "
            f"{total_recency} recency</p>",

            df_to_html_table(crit_table, "Critical records", max_rows=200),
            df_to_html_table(overflow_table, "Overflow threshold configurations", max_rows=500),
            df_to_html_table(recency_table, "Recency monitoring flags", max_rows=500),
            "<hr/>",
            f"<p class='muted'>Back to main report: <a href='../report.html'>report.html</a></p>",
            "</body></html>",
        ]

        fname = safe_filename(gname) + ".html"
        (pages_dir / fname).write_text("\n".join(parts), encoding="utf-8")
