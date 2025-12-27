"""
Radar Visualization Runner

Main entry point for radar visualization pipeline.

Functions:
    run_radar_visual_report: Generate radar dashboard

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from moata_pipeline.common.file_utils import ensure_dir
from .radar_cleaning import load_and_analyze
from .radar_report import build_radar_dashboard


__version__ = "1.0.0"


def run_radar_visual_report(
    data_dir: Path,
    out_dir: Optional[Path] = None,
    data_date: Optional[str] = None,
) -> Path:
    """
    Run radar visualization pipeline.
    
    Args:
        data_dir: Path to radar raw data directory
        out_dir: Output directory (default: data_dir/../dashboard)
        data_date: Optional date string for display
        
    Returns:
        Path to generated HTML report
        
    Raises:
        FileNotFoundError: If data directory not found
        ValueError: If no data to visualize
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Starting radar visual report")
    logger.info(f"Data directory: {data_dir}")
    
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    if out_dir is None:
        out_dir = data_dir.parent / "dashboard"
    
    ensure_dir(out_dir)
    logger.info(f"Output directory: {out_dir}")
    
    logger.info("Loading and analyzing radar data...")
    df = load_and_analyze(data_dir)
    
    if df.empty:
        raise ValueError("No data to visualize")
    
    logger.info(f"Loaded {len(df)} catchment records")
    
    stats_path = out_dir / "catchment_stats.csv"
    df.to_csv(stats_path, index=False)
    logger.info(f"✓ Saved stats to {stats_path}")
    
    report_path = build_radar_dashboard(df, out_dir, data_date=data_date)
    
    logger.info(f"✓ Radar visual report complete: {report_path}")
    return report_path