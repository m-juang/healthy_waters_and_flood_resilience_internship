"""
Visualization Report Module

Generates main HTML report for rain gauge alarm configuration.

Functions:
    build_report: Generate main HTML report with summary and links

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import html
import logging
from pathlib import Path

import pandas as pd

from moata_pipeline.common.text_utils import safe_filename
from moata_pipeline.common.html_utils import df_to_html_table


__version__ = "1.0.0"


# Report CSS styles
REPORT_CSS = """
<style>
  body { 
    font-family: Arial, sans-serif; 
    margin: 24px; 
    line-height: 1.4;
    background: #f5f5f5;
  }
  h1 { 
    margin-bottom: 6px;
    color: #1a1a1a;
  }
  h2 { 
    margin-top: 32px; 
    color: #333;
    border-bottom: 2px solid #2c5282;
    padding-bottom: 8px;
  }
  .muted { 
    color: #555; 
  }
  .note { 
    background: #e8f4fd; 
    border-left: 4px solid #2c5282; 
    padding: 12px 16px; 
    margin: 16px 0; 
    border-radius: 4px; 
  }
  table { 
    border-collapse: collapse; 
    width: 100%; 
    margin-top: 12px;
    background: white;
  }
  th, td { 
    border: 1px solid #ddd; 
    padding: 8px 10px; 
    font-size: 0.95em; 
  }
  th { 
    background: #f3f3f3; 
    text-align: left;
    font-weight: bold;
  }
  tr:nth-child(even) { 
    background: #fafafa; 
  }
  .stats { 
    display: flex; 
    gap: 24px; 
    flex-wrap: wrap; 
    margin: 16px 0; 
  }
  .stat-box { 
    background: white; 
    border: 1px solid #e0e0e0; 
    border-radius: 8px; 
    padding: 12px 20px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  }
  .stat-box .num { 
    font-size: 1.8em; 
    font-weight: bold; 
    color: #2c5282; 
  }
  .stat-box .label { 
    color: #666; 
    font-size: 0.9em; 
  }
  a {
    color: #2c5282;
    text-decoration: none;
  }
  a:hover {
    text-decoration: underline;
  }
</style>
"""


def build_report(df: pd.DataFrame, out_dir: Path) -> None:
    """
    Generate main HTML report for rain gauge alarm configuration.
    
    Creates comprehensive report with:
        - Summary statistics
        - Per-gauge overview table
        - All overflow alarms
        - All recency monitors
        - Links to individual gauge pages
        
    Args:
        df: DataFrame with alarm data (must have columns: Gauge, Trace,
            Alarm Name, Threshold, row_category)
        out_dir: Output directory for report
        
    Raises:
        ValueError: If DataFrame is empty or missing required columns
        
    Example:
        >>> df = load_and_clean(csv_path)
        >>> build_report(df, Path("outputs/rain_gauges/visualizations"))
        # Creates outputs/rain_gauges/visualizations/report.html
    """
    logger = logging.getLogger(__name__)
    
    if df.empty:
        raise ValueError("DataFrame is empty - cannot generate report")
    
    required_cols = ["Gauge", "Trace", "Alarm Name", "Threshold", "row_category"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    
    logger.info("Building main report...")
    
    # Calculate summary statistics
    total_rows = len(df)
    gauges_count = df["Gauge"].nunique()
    trace_types_count = df["Trace"].nunique()
    
    # === OVERFLOW TABLE ===
    overflow = df[df["row_category"] == "Threshold alarm (overflow)"].copy()
    overflow_table = (
        overflow[["Gauge", "Trace", "Alarm Name", "Threshold"]]
        .drop_duplicates()
        .sort_values(
            by=["Gauge", "Trace", "Threshold"],
            ascending=[True, True, True],
            na_position="last"
        )
    )
    
    # === RECENCY TABLE ===
    recency = df[df["row_category"] == "Data freshness (recency)"].copy()
    recency_table = (
        recency[["Gauge", "Trace", "Alarm Name", "Threshold"]]
        .drop_duplicates()
        .sort_values(
            by=["Gauge", "Threshold"],
            ascending=[True, False],
            na_position="last"
        )
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
    for gauge_name in gauges_list:
        link = f"gauge_pages/{safe_filename(gauge_name)}.html"
        link_rows.append({
            "Gauge": gauge_name,
            "Open": f"<a href='{html.escape(link)}'>View Details</a>"
        })
    links_df = pd.DataFrame(link_rows)
    
    # === BUILD HTML ===
    html_parts = [
        "<html>",
        "<head>",
        "<meta charset='utf-8'/>",
        "<title>Rain Gauge Alarm Configuration</title>",
        REPORT_CSS,
        "</head>",
        "<body>",
        
        # Header
        "<h1>Rain Gauge Alarm Configuration</h1>",
        "<p class='muted'>Active rain gauges with configured alarms</p>",
        
        # Statistics boxes
        "<div class='stats'>",
        f"<div class='stat-box'><div class='num'>{gauges_count}</div><div class='label'>Rain Gauges</div></div>",
        f"<div class='stat-box'><div class='num'>{trace_types_count}</div><div class='label'>Trace Types</div></div>",
        f"<div class='stat-box'><div class='num'>{len(overflow_table)}</div><div class='label'>Overflow Alarms</div></div>",
        f"<div class='stat-box'><div class='num'>{len(recency_table)}</div><div class='label'>Recency Monitors</div></div>",
        "</div>",
        
        # Summary table
        "<h2>Summary per Gauge</h2>",
        "<p class='muted'>Overview of each gauge with alarm counts.</p>",
        summary.to_html(index=False, escape=True, border=0),
        
        # Overflow alarms
        "<h2>Overflow/Threshold Alarms</h2>",
        "<p class='muted'>Alarms triggered when rainfall exceeds a threshold.</p>",
        df_to_html_table(overflow_table, "", max_rows=1000),
        
        # Recency monitoring
        "<h2>Data Freshness (Recency) Monitoring</h2>",
        "<div class='note'>",
        "<b>Note:</b> \"Hours Since Last Data\" shows how long ago the gauge last reported data ",
        "when this report was generated.",
        "</div>",
        df_to_html_table(recency_table, "", max_rows=500),
        
        # Gauge page links
        "<h2>Individual Gauge Pages</h2>",
        "<p class='muted'>Click to view details for each gauge.</p>",
        links_df.to_html(index=False, escape=False, border=0),
        
        "</body>",
        "</html>",
    ]
    
    # Write report
    report_path = out_dir / "report.html"
    report_path.write_text("\n".join(html_parts), encoding="utf-8")
    
    logger.info(f"✓ Main report saved to {report_path}")