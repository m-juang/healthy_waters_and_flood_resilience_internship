#!/usr/bin/env python3
"""
Rain Radar Data Visualization Script

Generates interactive HTML dashboard for radar QPE (Quantitative Precipitation 
Estimation) data analysis with charts, maps, and catchment statistics.

Features:
    - Interactive HTML dashboard with embedded charts
    - Per-catchment ARI heatmaps
    - Temporal rainfall patterns
    - ARI exceedance highlighting
    - Statistical summaries

Usage:
    # Auto-detect most recent data (prefers historical)
    python visualize_rain_radar.py
    
    # Visualize specific historical date
    python visualize_rain_radar.py --date 2025-05-09
    
    # Visualize current (last 24h) data
    python visualize_rain_radar.py --current
    
    # Visualize custom directory
    python visualize_rain_radar.py --data-dir outputs/rain_radar/raw
    
    # Custom output directory
    python visualize_rain_radar.py --date 2025-05-09 --output-dir custom/viz/

Output:
    outputs/rain_radar/dashboard/                      (for current data)
    outputs/rain_radar/historical/DATE/dashboard/      (for historical data)
    ├── radar_dashboard.html      # Main interactive dashboard
    ├── catchment_stats.csv       # Statistical summary
    └── charts/                   # Generated chart images

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.viz.radar_runner import run_radar_visual_report


# Version info
__version__ = "1.0.0"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Generate interactive HTML dashboard for rain radar data",
        epilog="""
Examples:
  # Auto-detect most recent data (prefers historical)
  %(prog)s
  
  # Visualize specific historical date
  %(prog)s --date 2025-05-09
  
  # Visualize current (last 24h) data explicitly
  %(prog)s --current
  
  # Visualize custom radar data directory
  %(prog)s --data-dir outputs/rain_radar/raw
  
  # Custom output directory
  %(prog)s --date 2025-05-09 --output-dir custom/dashboard/
  
  # Verbose logging for debugging
  %(prog)s --date 2025-05-09 --log-level DEBUG

Notes:
  - Auto-detection prefers historical data over current
  - Requires prior data collection (run retrieve_rain_radar.py first)
  - Dashboard opens in default web browser
  - Duration: ~5-7 minutes for full day of data
  - Requires: ~1 GB RAM for processing
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Data source options (mutually exclusive)
    source_group = parser.add_argument_group('Data Source (choose one or auto-detect)')
    source_mutex = source_group.add_mutually_exclusive_group()
    
    source_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Visualize historical data for specific date. "
             "Example: --date 2025-05-09"
    )
    
    source_mutex.add_argument(
        "--current",
        action="store_true",
        help="Visualize current (last 24h) data explicitly. "
             "Without this or --date, auto-detects most recent."
    )
    
    source_mutex.add_argument(
        "--data-dir",
        metavar="PATH",
        help="Path to custom radar raw data directory. "
             "Overrides --date and --current. "
             "Example: --data-dir outputs/rain_radar/raw"
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    
    output_group.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Custom output directory path. "
             "Default: auto-determined based on input location. "
             "Example: --output-dir custom/dashboard/"
    )
    
    # Logging options
    log_group = parser.add_argument_group('Logging Options')
    
    log_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO). "
             "Use DEBUG for verbose output."
    )
    
    # Metadata
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    return parser.parse_args()


def detect_data_dir(args: argparse.Namespace, logger: logging.Logger) -> tuple[Path, Optional[str]]:
    """
    Detect radar data directory and date based on arguments.
    
    Args:
        args: Parsed command-line arguments
        logger: Logger instance
        
    Returns:
        Tuple of (data_directory_path, date_string)
        
    Raises:
        FileNotFoundError: If data directory doesn't exist
    """
    data_date: Optional[str] = None
    
    # Option 1: Custom directory
    if args.data_dir:
        data_dir = Path(args.data_dir)
        logger.info("Using custom data directory: %s", data_dir)
        
    # Option 2: Specific date (historical)
    elif args.date:
        data_dir = Path(f"outputs/rain_radar/historical/{args.date}/raw")
        data_date = args.date
        logger.info("Using historical data for date: %s", args.date)
        
    # Option 3: Current data (explicit)
    elif args.current:
        data_dir = Path("outputs/rain_radar/raw")
        logger.info("Using current (last 24h) data")
        
    # Option 4: Auto-detect (prefer historical)
    else:
        logger.info("Auto-detecting radar data directory...")
        
        # Check for historical data
        historical_base = Path("outputs/rain_radar/historical")
        if historical_base.exists():
            historical_dirs = sorted(historical_base.glob("*/raw"))
            if historical_dirs:
                data_dir = historical_dirs[-1]  # Most recent
                data_date = data_dir.parent.name
                logger.info("✓ Found historical data: %s (date: %s)", data_dir, data_date)
            else:
                # Fallback to current
                data_dir = Path("outputs/rain_radar/raw")
                logger.info("No historical data found, using current: %s", data_dir)
        else:
            # Fallback to current
            data_dir = Path("outputs/rain_radar/raw")
            logger.info("Using current data: %s", data_dir)
    
    # Validate directory exists
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Data directory not found: {data_dir}\n\n"
            f"Have you run data collection first?\n"
            f"  For current data:    python retrieve_rain_radar.py\n"
            f"  For specific date:   python retrieve_rain_radar.py --date {args.date or '2025-05-09'}"
        )
    
    # Check for required subdirectories
    required_subdirs = ["radar_data", "pixel_mappings", "catchments"]
    missing = [d for d in required_subdirs if not (data_dir / d).exists()]
    
    if missing:
        raise FileNotFoundError(
            f"Incomplete data directory: {data_dir}\n"
            f"Missing subdirectories: {', '.join(missing)}\n\n"
            f"The directory exists but appears to be incomplete.\n"
            f"Try running data collection again."
        )
    
    # Check for data files
    radar_files = list((data_dir / "radar_data").glob("*.json"))
    if not radar_files:
        raise FileNotFoundError(
            f"No radar data files found in: {data_dir / 'radar_data'}\n"
            f"Directory structure exists but contains no data files."
        )
    
    logger.info(f"✓ Found {len(radar_files)} radar data files")
    
    return data_dir, data_date


def main() -> int:
    """
    Main entry point for radar data visualization.
    
    Returns:
        Exit code (0=success, 1=error, 130=interrupted)
    """
    # Parse arguments
    try:
        args = parse_args()
    except SystemExit as e:
        return e.code if e.code is not None else 0
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Rain Radar Dashboard Generator - v%s", __version__)
        logger.info("=" * 80)
        
        # Detect data directory
        data_dir, data_date = detect_data_dir(args, logger)
        
        # Determine output directory
        if args.output_dir:
            output_dir = Path(args.output_dir)
            logger.info("Using custom output directory: %s", output_dir)
        else:
            output_dir = None
            logger.info("Output directory: auto-determined")
        
        logger.info("")
        logger.info("Configuration:")
        logger.info(f"  Input:  {data_dir}")
        if data_date:
            logger.info(f"  Date:   {data_date}")
        if output_dir:
            logger.info(f"  Output: {output_dir}")
        logger.info("=" * 80)
        logger.info("")
        
        # Run visualization
        logger.info("Generating radar dashboard...")
        
        report_path = run_radar_visual_report(
            data_dir=data_dir,
            out_dir=output_dir,
            data_date=data_date,
        )
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Dashboard generated successfully")
        logger.info("=" * 80)
        logger.info(f"Dashboard: {report_path}")
        logger.info("")
        logger.info("To view:")
        logger.info(f"  Open in browser: {report_path.absolute()}")
        logger.info("  Or double-click the file in File Explorer")
        logger.info("=" * 80)
        
        # Print to stdout for easy access
        print(f"\n✅ Done! Open in browser: {report_path.absolute()}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("⚠️  Visualization interrupted by user (Ctrl+C)")
        logger.warning("=" * 80)
        return 130
        
    except FileNotFoundError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Data Not Found")
        logger.error("=" * 80)
        logger.error(str(e))
        return 1
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Visualization Failed")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("1. Verify radar data was collected successfully")
        logger.error("2. Check data directory has complete structure")
        logger.error("3. Ensure matplotlib and other dependencies are installed")
        logger.error("4. Check disk space for output files")
        logger.error("5. Try with --log-level DEBUG for more details")
        return 1


if __name__ == "__main__":
    sys.exit(main())