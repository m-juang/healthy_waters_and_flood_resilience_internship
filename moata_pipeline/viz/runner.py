"""
Visualization Runner Module

Provides high-level entry point for generating rain gauge visualization reports.

Functions:
    run_visual_report: Generate complete HTML visualization report from CSV data

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.common.file_utils import ensure_dir
from .cleaning import load_and_clean
from .pages import build_gauge_pages
from .report import build_report


# Version info
__version__ = "1.0.0"


# =============================================================================
# Custom Exceptions
# =============================================================================

class VisualizationError(Exception):
    """Base exception for visualization errors."""
    pass


# =============================================================================
# Main Runner Function
# =============================================================================

def run_visual_report(
    csv_path: Optional[Path] = None,
    out_dir: Optional[Path] = None,
) -> Path:
    """
    Generate complete HTML visualization report from alarm CSV data.
    
    Pipeline Steps:
        1. Load and clean CSV data
        2. Save cleaned CSV copy
        3. Build per-gauge HTML pages
        4. Build main report HTML
        
    Args:
        csv_path: Path to alarm summary CSV (default: auto-detect)
        out_dir: Output directory (default: outputs/rain_gauges/visualizations)
        
    Returns:
        Path to generated report.html
        
    Raises:
        FileNotFoundError: If CSV file not found
        VisualizationError: If visualization generation fails
        
    Example:
        >>> report_path = run_visual_report()
        >>> print(f"Report: {report_path}")
        Report: outputs/rain_gauges/visualizations/report.html
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("Rain Gauge Visualization Report")
    logger.info("=" * 80)
    
    # Initialize paths
    paths = PipelinePaths()
    csv_path = Path(csv_path) if csv_path else paths.alarm_summary_csv
    out_dir = Path(out_dir) if out_dir else paths.rain_gauges_viz_dir
    
    logger.info(f"Input CSV: {csv_path}")
    logger.info(f"Output directory: {out_dir}")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        # Step 1: Validate input
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Alarm summary CSV not found: {csv_path}\n\n"
                f"Run analysis first:\n"
                f"  python analyze_rain_gauges.py"
            )
        
        # Ensure output directory exists
        ensure_dir(out_dir)
        
        # Step 2: Load and clean data
        logger.info("Step 1: Loading and cleaning CSV data...")
        df = load_and_clean(csv_path)
        logger.info(f"✓ Loaded {len(df)} alarm records")
        
        if df.empty:
            logger.warning("CSV is empty - no alarms to visualize")
            raise VisualizationError(
                f"No alarm data found in {csv_path}\n"
                f"The CSV file is empty or contains no valid records."
            )
        
        # Step 3: Save cleaned copy
        logger.info("")
        logger.info("Step 2: Saving cleaned data...")
        cleaned_path = out_dir / "cleaned_alarm_summary.csv"
        df.to_csv(cleaned_path, index=False)
        logger.info(f"✓ Saved cleaned CSV: {cleaned_path}")
        
        # Step 4: Build per-gauge pages
        logger.info("")
        logger.info("Step 3: Building per-gauge HTML pages...")
        build_gauge_pages(df, out_dir)
        logger.info("✓ Per-gauge pages complete")
        
        # Step 5: Build main report
        logger.info("")
        logger.info("Step 4: Building main report...")
        build_report(df, out_dir)
        logger.info("✓ Main report complete")
        
        # Final output path
        report_path = out_dir / "report.html"
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Visualization Complete")
        logger.info("=" * 80)
        logger.info(f"Report: {report_path}")
        logger.info(f"Output directory: {out_dir}")
        logger.info("=" * 80)
        
        return report_path
        
    except FileNotFoundError as e:
        logger.error(str(e))
        raise
        
    except Exception as e:
        logger.error(f"Visualization failed: {e}")
        logger.exception("Full traceback:")
        raise VisualizationError(
            f"Failed to generate visualization: {e}"
        ) from e