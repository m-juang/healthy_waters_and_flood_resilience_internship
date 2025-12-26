"""
Entry point for rain radar data analysis.

Analyzes radar data for stormwater catchments and calculates ARI values.

Usage:
    python analyze_rain_radar.py                        # Auto-detect (prefer historical)
    python analyze_rain_radar.py --date 2025-05-09      # Analyze specific date
    python analyze_rain_radar.py --current              # Analyze current (last 24h) data
    python analyze_rain_radar.py --data-dir PATH        # Analyze specific directory

Output:
    outputs/rain_radar/[analyze|historical/DATE/analyze]/
    ├── ari_analysis_summary.csv
    ├── ari_exceedances.csv
    └── analysis_report.txt
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.analyze.radar_analysis import run_radar_analysis


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze rain radar data and calculate ARI")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Analyze historical data for specific date",
    )
    parser.add_argument(
        "--current",
        action="store_true",
        help="Analyze current (last 24h) data instead of historical",
    )
    parser.add_argument(
        "--data-dir",
        metavar="PATH",
        help="Path to radar data directory (overrides --date and --current)",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Path to output directory (default: auto based on input)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="ARI threshold in years (default: 5.0)",
    )
    
    args = parser.parse_args()
    
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("RAIN RADAR ARI ANALYSIS")
    logger.info("=" * 80)
    
    # Determine radar data directory
    if args.data_dir:
        radar_data_dir = Path(args.data_dir)
    elif args.date:
        radar_data_dir = Path(f"outputs/rain_radar/historical/{args.date}/raw/radar_data")
    elif args.current:
        radar_data_dir = Path("outputs/rain_radar/raw/radar_data")
    else:
        # Auto-detect: prefer historical data if exists
        historical_dirs = sorted(Path("outputs/rain_radar/historical").glob("*/raw/radar_data"))
        if historical_dirs:
            radar_data_dir = historical_dirs[-1]  # Most recent historical
            logger.info("Auto-detected historical data: %s", radar_data_dir)
        else:
            radar_data_dir = Path("outputs/rain_radar/raw/radar_data")
            logger.info("Using current data: %s", radar_data_dir)
    
    if not radar_data_dir.exists():
        logger.error("Radar data directory not found: %s", radar_data_dir)
        logger.error("Run 'python retrieve_rain_radar.py' first")
        if args.date:
            logger.error("Or run 'python retrieve_rain_radar.py --date %s'", args.date)
        return
    
    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Put analyze output next to raw data
        output_dir = radar_data_dir.parent.parent / "analyze"
    
    logger.info("Input directory: %s", radar_data_dir)
    logger.info("Output directory: %s", output_dir)
    logger.info("ARI threshold: %.1f years", args.threshold)
    
    # Run analysis
    result = run_radar_analysis(
        radar_data_dir=radar_data_dir,
        output_dir=output_dir,
        ari_threshold=args.threshold,
    )
    
    logger.info("\n%s", result["report"])
    
    logger.info("=" * 80)
    logger.info("COMPLETE!")
    logger.info("=" * 80)
    logger.info("Output files saved to: %s", result["output_dir"])
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
