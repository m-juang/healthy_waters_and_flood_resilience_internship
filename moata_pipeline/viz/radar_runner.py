"""
Runner for radar visualization.
"""
from __future__ import annotations

import logging
from pathlib import Path

from moata_pipeline.common.file_utils import ensure_dir
from .radar_cleaning import load_and_analyze
from .radar_report import build_radar_dashboard

logger = logging.getLogger(__name__)


def run_radar_visual_report(
    data_dir: Path,
    out_dir: Path | None = None,
    data_date: str | None = None,
) -> Path:
    """
    Run radar visualization pipeline.
    
    Args:
        data_dir: Path to radar raw data directory
        out_dir: Output directory (default: data_dir/../dashboard)
        data_date: Optional date string for display
        
    Returns:
        Path to generated HTML report
    """
    logger.info("Starting radar visual report")
    logger.info("Data directory: %s", data_dir)
    
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    # Default output directory
    if out_dir is None:
        out_dir = data_dir.parent / "dashboard"
    
    ensure_dir(out_dir)
    logger.info("Output directory: %s", out_dir)
    
    # Load and analyze data
    logger.info("Loading and analyzing radar data...")
    df = load_and_analyze(data_dir)
    
    if df.empty:
        raise ValueError("No data to visualize")
    
    logger.info("Loaded %d catchment records", len(df))
    
    # Save cleaned stats CSV
    stats_path = out_dir / "catchment_stats.csv"
    df.to_csv(stats_path, index=False)
    logger.info("✓ Saved stats to %s", stats_path)
    
    # Build dashboard
    report_path = build_radar_dashboard(df, out_dir, data_date=data_date)
    
    logger.info("✓ Radar visual report complete: %s", report_path)
    return report_path
