#!/usr/bin/env python3
"""
Rain Radar Data Collection Script

Collects rain radar (QPE - Quantitative Precipitation Estimation) data from 
Moata API for Auckland Council stormwater catchments.

Features:
    - Current data collection (last 24 hours)
    - Historical data collection (specific dates or date ranges)
    - Automatic pixel mapping generation
    - Progress tracking and logging
    - Error recovery and retry logic

Usage:
    # Collect last 24 hours (current data)
    python retrieve_rain_radar.py
    
    # Collect specific date (historical)
    python retrieve_rain_radar.py --date 2025-05-09
    
    # Collect date range
    python retrieve_rain_radar.py --start 2025-05-09 --end 2025-05-10
    
    # Force refresh pixel mappings
    python retrieve_rain_radar.py --force-refresh-pixels
    
    # Verbose logging
    python retrieve_rain_radar.py --date 2025-05-09 --log-level DEBUG

Output:
    Current data: outputs/rain_radar/raw/
    Historical:   outputs/rain_radar/historical/YYYY-MM-DD/raw/

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23 (FIXED: --date parameter now works correctly)
Version: 1.0.1
"""

import sys
from pathlib import Path
# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.collect.runner import run_collect_radar


# Version info
__version__ = "1.0.1"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Collect rain radar (QPE) data from Moata API for Auckland catchments",
        epilog="""
Examples:
  # Collect last 24 hours (current data)
  %(prog)s
  
  # Collect specific date (full 24 hours)
  %(prog)s --date 2025-05-09
  
  # Force refresh pixel mappings from API
  %(prog)s --force-refresh-pixels
  
  # Verbose logging for debugging
  %(prog)s --date 2025-05-09 --log-level DEBUG

Notes:
  - Data collection uses specified date for 24-hour period (00:00 to 23:59 UTC)
  - Example: --date 2025-05-09 fetches 2025-05-09 00:00:00 to 2025-05-10 00:00:00
  - Pixel mappings are cached unless --force-refresh-pixels is used
  - Collection duration: ~15-30 minutes
  - Requires: ~2-4 GB RAM for processing
  - Output size: ~500 MB - 2 GB
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Date options
    date_group = parser.add_argument_group('Date Options (default: last 24 hours)')
    
    date_group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Date to collect (fetches full 24 hours). Example: --date 2025-05-09"
    )
    
    # Processing options
    proc_group = parser.add_argument_group('Processing Options')
    
    proc_group.add_argument(
        "--force-refresh-pixels",
        action="store_true",
        help="Force rebuild pixel mappings from API. "
             "Normally cached pixel mappings are reused. "
             "Use this if catchment boundaries have changed."
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
    
    args = parser.parse_args()
    
    return args


def parse_date(date_str: str, param_name: str) -> datetime:
    """
    Parse date string to datetime.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        param_name: Parameter name for error messages
        
    Returns:
        Parsed datetime at start of day (00:00:00 UTC)
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return datetime(
            date.year, date.month, date.day, 
            0, 0, 0, 
            tzinfo=timezone.utc
        )
    except ValueError as e:
        raise ValueError(
            f"Invalid {param_name} format: '{date_str}'. "
            f"Expected YYYY-MM-DD (e.g., 2025-05-09). Error: {e}"
        ) from e


def validate_date_range(start_time: datetime, end_time: datetime) -> None:
    """
    Validate date range is logical.
    
    Args:
        start_time: Start datetime
        end_time: End datetime
        
    Raises:
        ValueError: If date range is invalid
    """
    if start_time >= end_time:
        raise ValueError(
            f"Start date must be before end date. "
            f"Got start={start_time.date()}, end={end_time.date()}"
        )
    
    # Check if range is too large (warn, don't error)
    duration = (end_time - start_time).days
    if duration > 31:
        logging.warning(
            f"⚠️  Large date range: {duration} days. "
            f"This may take a long time and use significant disk space."
        )


def main() -> int:
    """
    Main entry point for radar data collection.
    
    Returns:
        Exit code (0=success, 1=error, 130=interrupted)
    """
    # Parse arguments
    try:
        args = parse_args()
    except SystemExit as e:
        # argparse calls sys.exit() for --help or errors
        return e.code if e.code is not None else 0
    
    # Setup logging
    logger = setup_script_logger(args.log_level, __name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Rain Radar Data Collection - v%s", __version__)
        logger.info("=" * 80)
        
        # Determine time range and paths
        start_time: Optional[datetime] = None
        end_time: Optional[datetime] = None
        mode: str = "current"
        paths: PipelinePaths
        
        if args.date:
            # ✅ FIXED: Parse tanggal yang diminta user
            logger.info("Mode: Historical (specific date)")
            logger.info(f"Requested date: {args.date}")
            
            # Parse tanggal dari string
            requested_date = parse_date(args.date, "--date")
            
            # Buat range 24 jam untuk tanggal tersebut
            # Contoh: 2025-05-09 → dari 2025-05-09 00:00:00 sampai 2025-05-10 00:00:00
            start_time = requested_date
            end_time = start_time + timedelta(hours=24)
            
            mode = "historical"
            logger.info(f"Date range: {start_time.date()} to {end_time.date()}")
            paths = PipelinePaths.for_date_range(start_time, end_time)
            
        else:
            # Default: last 24 hours (current/today)
            logger.info("Mode: Current (last 24 hours)")
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=24)
            mode = "current"
            paths = PipelinePaths.for_date_range(start_time, end_time)
        
        # Log time range
        logger.info(f"Start time (UTC): {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time (UTC):   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Log processing options
        if args.force_refresh_pixels:
            logger.info("🔄 Pixel mappings: Force refresh from API")
        else:
            logger.info("📦 Pixel mappings: Use cached (if available)")
        
        logger.info("=" * 80)
        logger.info("")
        
        # Run collection
        logger.info("Starting radar data collection...")
        
        run_collect_radar(
            start_time=start_time,
            end_time=end_time,
            force_refresh_pixels=args.force_refresh_pixels,
        )
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Radar data collection completed successfully")
        logger.info("=" * 80)
        
        # Log output location
        logger.info(f"Output location: {paths.rain_radar_raw_dir}/")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("⚠️  Collection interrupted by user (Ctrl+C)")
        logger.warning("=" * 80)
        logger.warning("Partial data may have been saved.")
        logger.warning("You can resume by running the script again.")
        return 130
        
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
        logger.error("❌ Collection Failed")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("1. Check your internet connection")
        logger.error("2. Verify API credentials in .env file")
        logger.error("3. Check disk space (need ~5 GB for historical data)")
        logger.error("4. Try with --log-level DEBUG for more details")
        logger.error("5. Check if date is valid (historical data from 2024-01-01)")
        return 1


if __name__ == "__main__":
    sys.exit(main())