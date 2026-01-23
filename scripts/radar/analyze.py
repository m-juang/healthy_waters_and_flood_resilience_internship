#!/usr/bin/env python3
"""
Rain Radar Data Analysis Script

Analyzes radar QPE (Quantitative Precipitation Estimation) data for Auckland 
stormwater catchments and calculates ARI (Average Recurrence Interval) values.

Features:
    - Automatic data directory detection
    - ARI calculation using TP108 methodology
    - Catchment-level aggregation
    - ARI exceedance identification
    - Summary statistics and reporting

Usage:
    # Auto-detect most recent data (prefers historical)
    python analyze_rain_radar.py
    
    # Analyze specific historical date
    python analyze_rain_radar.py --date 2025-05-09
    
    # Analyze current (last 24h) data
    python analyze_rain_radar.py --current
    
    # Analyze custom directory
    python analyze_rain_radar.py --data-dir outputs/rain_radar/raw/radar_data
    
    # Custom ARI threshold and output
    python analyze_rain_radar.py --date 2025-05-09 --threshold 10.0 --output-dir custom/

Output:
    outputs/rain_radar/analysis/                    (for current data)
    outputs/rain_radar/YYYY/MM/DD/analysis/         (for historical data)
    +-- ari_analysis_summary.csv      # Per-catchment ARI summary
    +-- ari_exceedances.csv           # Catchments exceeding threshold
    +-- analysis_report.txt           # Detailed text report

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-20
Version: 1.1.0
"""

import sys
from pathlib import Path
# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.analyze.radar_analysis import run_radar_analysis


# Version info
__version__ = "1.1.0"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Analyze rain radar (QPE) data and calculate ARI for catchments",
        epilog="""
Examples:
  # Auto-detect most recent data (prefers historical)
  %(prog)s
  
  # Analyze specific historical date
  %(prog)s --date 2025-05-09
  
  # Analyze current (last 24h) data explicitly
  %(prog)s --current
  
  # Analyze custom radar data directory
  %(prog)s --data-dir outputs/rain_radar/raw/radar_data
  
  # Custom ARI threshold (10-year instead of default 5-year)
  %(prog)s --date 2025-05-09 --threshold 10.0
  
  # Generate alarm timeline
  %(prog)s --date 2025-05-09 --alarm-timeline
  
  # Custom output directory
  %(prog)s --current --output-dir custom/analysis/
  
  # Verbose logging for debugging
  %(prog)s --date 2025-05-09 --log-level DEBUG

Notes:
  - Auto-detection prefers historical data over current
  - Analysis requires prior data collection (run retrieve_rain_radar.py first)
  - ARI threshold determines which catchments are flagged
  - Duration: ~10-15 minutes for full day of data
  - Requires: ~2 GB RAM for processing
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Data source options (mutually exclusive)
    source_group = parser.add_argument_group('Data Source (choose one or auto-detect)')
    source_mutex = source_group.add_mutually_exclusive_group()
    
    source_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Analyze historical data for specific date. "
             "Example: --date 2025-05-09"
    )
    
    source_mutex.add_argument(
        "--current",
        action="store_true",
        help="Analyze current (last 24h) data explicitly. "
             "Without this or --date, auto-detects most recent."
    )
    
    source_mutex.add_argument(
        "--data-dir",
        metavar="PATH",
        help="Path to custom radar data directory. "
             "Overrides --date and --current. "
             "Example: --data-dir outputs/rain_radar/raw/radar_data"
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    
    output_group.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Custom output directory path. "
             "Default: auto-determined based on input location. "
             "Example: --output-dir custom/analysis/"
    )
    
    # Analysis options
    analysis_group = parser.add_argument_group('Analysis Options')
    
    analysis_group.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        metavar="YEARS",
        help="ARI threshold for exceedance recording (default: 5.0 years). "
             "Example: --threshold 10.0 for 10-year threshold"
    )
    
    analysis_group.add_argument(
        "--alarm-timeline",
        action="store_true",
        help="Generate alarm timeline showing when alarms would trigger. "
             "Checks alarm status at EVERY timestamp. "
             "Note: This significantly increases processing time (may add 5-15 minutes)."
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


def validate_threshold(threshold: float) -> None:
    """
    Validate ARI threshold value.
    
    Args:
        threshold: ARI threshold in years
        
    Raises:
        ValueError: If threshold is invalid
    """
    if threshold <= 0:
        raise ValueError(
            f"ARI threshold must be positive, got {threshold}"
        )
    
    if threshold > 100:
        logging.warning(
            f"⚠️  Very high ARI threshold: {threshold} years. "
            f"This may result in very few or no exceedances."
        )


def detect_radar_data_dir(args: argparse.Namespace, logger: logging.Logger) -> tuple[Path, PipelinePaths]:
    """
    Detect radar data directory based on arguments and return PipelinePaths instance.
    
    Args:
        args: Parsed command-line arguments
        logger: Logger instance
        
    Returns:
        Tuple of (radar_data_dir, PipelinePaths)
        
    Raises:
        FileNotFoundError: If data directory doesn't exist
    """
    # Option 1: Custom directory
    if args.data_dir:
        radar_data_dir = Path(args.data_dir)
        logger.info("Using custom data directory: %s", radar_data_dir)
        paths = PipelinePaths()  # Use today's date for custom dir
        
    # Option 2: Specific date (historical)
    elif args.date:
        # Try the specified date first
        paths = PipelinePaths.for_date(args.date)
        radar_data_dir = paths.rain_radar_data_dir
        
        # If no data found, try date-1 (because data is stored with window start date)
        if not radar_data_dir.exists() or not list(radar_data_dir.glob("*.csv")):
            from datetime import datetime, timedelta
            try:
                specified_date = datetime.strptime(args.date, "%Y-%m-%d")
                prev_date = (specified_date - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.warning(
                    f"No data found for {args.date}, trying previous date {prev_date}..."
                )
                paths = PipelinePaths.for_date(prev_date)
                radar_data_dir = paths.rain_radar_data_dir
                logger.info(f"Using historical data for date: {prev_date}")
            except Exception as e:
                logger.info("Using historical data for date: %s", args.date)
        else:
            logger.info("Using historical data for date: %s", args.date)
        
    # Option 3: Current data (explicit)
    elif args.current:
        paths = PipelinePaths.for_today()
        radar_data_dir = paths.rain_radar_data_dir
        logger.info("Using current (last 24h) data")
        
    # Option 4: Auto-detect (prefer historical)
    else:
        logger.info("Auto-detecting radar data directory...")
        
        # Try to find most recent historical data
        # Format: outputs/rain_radar/YYYYMMDD-YYYYMMDD/raw/radar_data
        historical_base = Path("outputs/rain_radar")
        historical_dirs = sorted(
            [d for d in historical_base.glob("*-*/raw/radar_data") if d.exists()],
            key=lambda p: p.parent.parent.name,  # Sort by date folder name
            reverse=True
        )
        
        if historical_dirs:
            radar_data_dir = historical_dirs[0]
            # Extract date range from path: outputs/rain_radar/YYYYMMDD-YYYYMMDD/raw/radar_data
            date_range_folder = radar_data_dir.parent.parent.name  # e.g., "20260120-20260121"
            logger.info(f"✓ Found historical data: {radar_data_dir} (range: {date_range_folder})")
            
            # Parse end date from folder name for PipelinePaths
            if "-" in date_range_folder and len(date_range_folder) == 17:
                end_date_str = date_range_folder.split("-")[1]  # YYYYMMDD
                date_str = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                paths = PipelinePaths.for_date(date_str)
            else:
                paths = PipelinePaths.for_today()
        else:
            # Fallback to current/today
            paths = PipelinePaths.for_today()
            radar_data_dir = paths.rain_radar_data_dir
            logger.info(f"Using current data: {radar_data_dir}")
    
    # Validate directory exists
    if not radar_data_dir.exists():
        raise FileNotFoundError(
            f"Radar data directory not found: {radar_data_dir}\n\n"
            f"Have you run data collection first?\n"
            f"  For current data:    python retrieve_rain_radar.py\n"
            f"  For specific date:   python retrieve_rain_radar.py --date {args.date or '2025-05-09'}"
        )
    
    # Check if directory has data
    data_files = list(radar_data_dir.glob("*.csv"))
    if not data_files:
        raise FileNotFoundError(
            f"No radar data files found in: {radar_data_dir}\n"
            f"Directory exists but is empty."
        )
    
    logger.info(f"✓ Found {len(data_files)} radar data files")
    
    return radar_data_dir, paths


def determine_output_dir(
    radar_data_dir: Path,
    paths: PipelinePaths,
    args: argparse.Namespace,
    logger: logging.Logger
) -> Path:
    """
    Determine output directory based on PipelinePaths instance.
    
    Args:
        radar_data_dir: Input radar data directory
        paths: PipelinePaths instance
        args: Parsed arguments
        logger: Logger instance
        
    Returns:
        Path to output directory
    """
    if args.output_dir:
        output_dir = Path(args.output_dir)
        logger.info("Using custom output directory: %s", output_dir)
    else:
        # Use analysis directory from PipelinePaths
        output_dir = paths.rain_radar_analysis_dir
        logger.info("Auto-determined output directory: %s", output_dir)
    
    return output_dir


def main() -> int:
    """
    Main entry point for radar data analysis.
    
    Returns:
        Exit code (0=success, 1=error, 130=interrupted)
    """
    # Parse arguments
    try:
        args = parse_args()
    except SystemExit as e:
        return e.code if e.code is not None else 0
    
    # Setup logging
    logger = setup_script_logger(args.log_level, __name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Rain Radar ARI Analysis - v%s", __version__)
        logger.info("=" * 80)
        
        # Validate threshold
        validate_threshold(args.threshold)
        logger.info(f"ARI threshold: {args.threshold} years")
        
        # Detect radar data directory and get PipelinePaths
        radar_data_dir, paths = detect_radar_data_dir(args, logger)
        
        # Determine output directory
        output_dir = determine_output_dir(radar_data_dir, paths, args, logger)
        
        logger.info("")
        logger.info("Configuration:")
        logger.info(f"  Input:     {radar_data_dir}")
        logger.info(f"  Output:    {output_dir}")
        logger.info(f"  Threshold: {args.threshold} years")
        logger.info("=" * 80)
        logger.info("")
        
        # Run analysis
        logger.info("Starting ARI analysis...")
        
        result = run_radar_analysis(
            radar_data_dir=radar_data_dir,
            output_dir=output_dir,
            ari_threshold=args.threshold,
            generate_alarm_timeline=args.alarm_timeline,
        )
        
        # Display report
        logger.info("")
        logger.info("=" * 80)
        logger.info("ANALYSIS RESULTS")
        logger.info("=" * 80)
        logger.info("")
        logger.info(result["report"])
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Analysis completed successfully")
        logger.info("=" * 80)
        logger.info(f"Output files saved to: {result['output_dir']}")
        logger.info("")
        logger.info("Generated files:")
        logger.info("  - ari_analysis_summary.csv  (per-catchment ARI summary)")
        logger.info("  - ari_exceedances.csv       (catchments exceeding threshold)")
        logger.info("  - analysis_report.txt       (detailed text report)")
        
        if args.alarm_timeline:
            logger.info("  - alarm_timeline.csv        (alarm status at each timestamp)")
            logger.info("  - alarm_summary.txt         (alarm duration analysis)")
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("⚠️  Analysis interrupted by user (Ctrl+C)")
        logger.warning("=" * 80)
        return 130
        
    except FileNotFoundError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Data Not Found")
        logger.error("=" * 80)
        logger.error(str(e))
        return 1
        
    except ValueError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Validation Error")
        logger.error("=" * 80)
        logger.error(str(e))
        logger.error("")
        logger.error("Run with --help for usage information.")
        return 1
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Analysis Failed")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("1. Verify radar data was collected successfully")
        logger.error("2. Check data directory exists and has .csv files")
        logger.error("3. Ensure TP108 coefficients file is available")
        logger.error("4. Check disk space for output files")
        logger.error("5. Try with --log-level DEBUG for more details")
        return 1


if __name__ == "__main__":
    sys.exit(main())