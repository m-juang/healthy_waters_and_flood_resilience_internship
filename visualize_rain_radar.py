"""
Entry point for rain radar data visualization.

Generates interactive HTML dashboard for radar data analysis.

Usage:
    python visualize_rain_radar.py                      # Auto-detect data
    python visualize_rain_radar.py --date 2025-05-09    # Specific historical date
    python visualize_rain_radar.py --current            # Current (last 24h) data

Output:
    outputs/rain_radar/[dashboard|historical/DATE/dashboard]/
    ├── radar_dashboard.html
    └── catchment_stats.csv
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.viz.radar_runner import run_radar_visual_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate rain radar dashboard")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Visualize historical data for specific date",
    )
    parser.add_argument(
        "--current",
        action="store_true",
        help="Visualize current (last 24h) data instead of historical",
    )
    parser.add_argument(
        "--data-dir",
        metavar="PATH",
        help="Path to radar raw data directory (overrides --date and --current)",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Path to output directory",
    )
    
    args = parser.parse_args()
    
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("RAIN RADAR DASHBOARD GENERATOR")
    logger.info("=" * 80)
    
    # Determine data directory and date
    data_date = None
    if args.data_dir:
        data_dir = Path(args.data_dir)
    elif args.date:
        data_dir = Path(f"outputs/rain_radar/historical/{args.date}/raw")
        data_date = args.date
    elif args.current:
        data_dir = Path("outputs/rain_radar/raw")
    else:
        # Auto-detect: prefer historical
        historical_dirs = sorted(Path("outputs/rain_radar/historical").glob("*/raw"))
        if historical_dirs:
            data_dir = historical_dirs[-1]
            data_date = data_dir.parent.name
            logger.info("Auto-detected historical data: %s", data_dir)
        else:
            data_dir = Path("outputs/rain_radar/raw")
            logger.info("Using current data: %s", data_dir)
    
    if not data_dir.exists():
        logger.error("Data directory not found: %s", data_dir)
        if args.date:
            logger.error("Run 'python retrieve_rain_radar.py --date %s' first", args.date)
        return
    
    # Output directory
    out_dir = Path(args.output_dir) if args.output_dir else None
    
    # Run visualization
    report_path = run_radar_visual_report(
        data_dir=data_dir,
        out_dir=out_dir,
        data_date=data_date,
    )
    
    logger.info("=" * 80)
    logger.info("COMPLETE!")
    logger.info("=" * 80)
    logger.info("Dashboard: %s", report_path)
    logger.info("=" * 80)
    
    print(f"\n✅ Done! Open in browser: {report_path}")


if __name__ == "__main__":
    main()
