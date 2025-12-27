"""
Visualization Pages Module

Generates individual HTML pages for each rain gauge with alarm details.

Functions:
    build_gauge_pages: Generate per-gauge HTML pages
    create_gauge_stats: Create statistics summary HTML
    create_overflow_section: Create overflow alarms section
    create_recency_section: Create recency monitoring section

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
from moata_pipeline.common.file_utils import ensure_dir
from moata_pipeline.common.html_utils import df_to_html_table


# Version info
__version__ = "1.0.0"

# Page CSS styles
PAGE_CSS = """
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
    margin-top: 28px; 
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
    gap: 20px; 
    flex-wrap: wrap; 
    margin: 16px 0; 
  }
  .stat-box { 
    background: white; 
    border: 1px solid #e0e0e0; 
    border-radius: 8px; 
    padding: 10px 16px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  }
  .stat-box .num { 
    font-size: 1.5em; 
    font-weight: bold; 
    color: #2c5282; 
  }
  .stat-box .label { 
    color: #666; 
    font-size: 0.85em; 
  }
  .back-link {
    margin-top: 20px;
    padding-top: 20px;
    border-top: 1px solid #ddd;
  }
</style>
"""


# =============================================================================
# Helper Functions
# =============================================================================

def create_gauge_stats(overflow_count: int, recency_count: int) -> str:
    """
    Create statistics summary HTML.
    
    Args:
        overflow_count: Number of overflow alarms
        recency_count: Number of recency monitors
        
    Returns:
        HTML string with stats boxes
    """
    return f"""
<div class='stats'>
    <div class='stat-box'>
        <div class='num'>{overflow_count}</div>
        <div class='label'>Overflow Alarms</div>
    </div>
    <div class='stat-box'>
        <div class='num'>{recency_count}</div>
        <div class='label'>Recency Monitors</div>
    </div>
</div>
"""


def create_overflow_section(overflow_table: pd.DataFrame) -> str:
    """
    Create overflow alarms section HTML.
    
    Args:
        overflow_table: DataFrame with overflow alarms
        
    Returns:
        HTML string with section
    """
    return f"""
<h2>Overflow/Threshold Alarms</h2>
{df_to_html_table(overflow_table, "", max_rows=500)}
"""


def create_recency_section(recency_table: pd.DataFrame) -> str:
    """
    Create recency monitoring section HTML.
    
    Args:
        recency_table: DataFrame with recency data
        
    Returns:
        HTML string with section
    """
    return f"""
<h2>Data Freshness (Recency)</h2>
<div class='note'>
    <b>Note:</b> "Hours Since Last Data" shows how long ago this gauge last reported data 
    when the report was generated.
</div>
{df_to_html_table(recency_table, "", max_rows=100)}
"""


# =============================================================================
# Main Page Generation Function
# =============================================================================

def build_gauge_pages(df: pd.DataFrame, out_dir: Path) -> None:
    """
    Generate individual HTML pages for each rain gauge.
    
    Creates a separate HTML page for each gauge showing:
        - Summary statistics
        - Overflow/threshold alarms
        - Data freshness (recency) monitoring
        
    Args:
        df: DataFrame with alarm data (must have columns: Gauge, Trace, 
            Alarm Name, Threshold, row_category)
        out_dir: Output directory for pages
        
    Raises:
        ValueError: If DataFrame is empty or missing required columns
        
    Example:
        >>> df = load_and_clean(csv_path)
        >>> build_gauge_pages(df, Path("outputs/rain_gauges/visualizations"))
        # Creates outputs/rain_gauges/visualizations/gauge_pages/*.html
    """
    logger = logging.getLogger(__name__)
    
    if df.empty:
        raise ValueError("DataFrame is empty - cannot generate gauge pages")
    
    required_cols = ["Gauge", "Trace", "Alarm Name", "Threshold", "row_category"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    
    # Create output directory
    pages_dir = out_dir / "gauge_pages"
    ensure_dir(pages_dir)
    
    logger.info(f"Generating per-gauge pages in {pages_dir}")
    
    # Get unique gauges
    gauges = sorted([g for g in df["Gauge"].unique() if str(g).strip() != ""])
    logger.info(f"Found {len(gauges)} gauges to process")
    
    for idx, gauge_name in enumerate(gauges, start=1):
        try:
            # Filter to this gauge
            gauge_df = df[df["Gauge"] == gauge_name].copy()
            
            # === OVERFLOW TABLE ===
            overflow = gauge_df[
                gauge_df["row_category"] == "Threshold alarm (overflow)"
            ].copy()
            
            overflow_table = (
                overflow[["Trace", "Alarm Name", "Threshold"]]
                .drop_duplicates()
                .sort_values(
                    by=["Trace", "Threshold"],
                    ascending=[True, True],
                    na_position="last"
                )
            )
            
            # === RECENCY TABLE ===
            recency = gauge_df[
                gauge_df["row_category"] == "Data freshness (recency)"
            ].copy()
            
            recency_table = (
                recency[["Trace", "Alarm Name", "Threshold"]]
                .drop_duplicates()
                .sort_values(by=["Trace"], ascending=True)
                .rename(columns={"Threshold": "Hours Since Last Data"})
            )
            
            # Count totals
            total_overflow = len(overflow_table)
            total_recency = len(recency_table)
            
            # === BUILD HTML ===
            html_parts = [
                "<html>",
                "<head>",
                "<meta charset='utf-8'/>",
                f"<title>{html.escape(gauge_name)}</title>",
                PAGE_CSS,
                "</head>",
                "<body>",
                f"<h1>{html.escape(gauge_name)}</h1>",
                
                # Statistics
                create_gauge_stats(total_overflow, total_recency),
                
                # Overflow section
                create_overflow_section(overflow_table),
                
                # Recency section
                create_recency_section(recency_table),
                
                # Back link
                "<div class='back-link'>",
                "<p class='muted'><a href='../report.html'>← Back to main report</a></p>",
                "</div>",
                
                "</body>",
                "</html>",
            ]
            
            # === WRITE FILE ===
            filename = safe_filename(gauge_name) + ".html"
            output_path = pages_dir / filename
            output_path.write_text("\n".join(html_parts), encoding="utf-8")
            
            if (idx % 10 == 0) or (idx == len(gauges)):
                logger.info(f"  Generated {idx}/{len(gauges)} pages...")
                
        except Exception as e:
            logger.warning(f"Failed to generate page for gauge '{gauge_name}': {e}")
            continue
    
    logger.info(f"✓ Generated {len(gauges)} gauge pages in {pages_dir}")