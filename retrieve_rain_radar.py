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
Last Modified: 2024-12-28
Version: 1.0.0
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.collect.runner import run_collect_radar


# Version info
__version__ = "1.0.0"


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
  
  # Collect specific historical date
  %(prog)s --date 2025-05-09
  
  # Collect date range (multiple days)
  %(prog)s --start 2025-05-09 --end 2025-05-12
  
  # Force refresh pixel mappings from API
  %(prog)s --force-refresh-pixels
  
  # Verbose logging for debugging
  %(prog)s --date 2025-05-09 --log-level DEBUG
  
  # Combine options
  %(prog)s --start 2025-05-01 --end 2025-05-07 --force-refresh-pixels --log-level INFO

Notes:
  - All dates are in UTC timezone
  - Historical data available from 2024-01-01 onwards
  - Current data collection fetches last 24 hours
  - Pixel mappings are cached unless --force-refresh-pixels is used
  - Collection duration: ~15-30 minutes depending on date range
  - Requires: ~2-4 GB RAM for processing
  - Output size: ~500 MB - 5 GB depending on duration
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Date options (mutually exclusive with default)
    date_group = parser.add_argument_group('Date Options (choose one)')
    date_mutex = date_group.add_mutually_exclusive_group()
    
    date_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Fetch data for specific date (full 24 hours UTC). "
             "Example: --date 2025-05-09"
    )
    
    date_group.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Start date for date range (UTC, inclusive). "
             "Requires --end. Example: --start 2025-05-09"
    )
    
    date_group.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        help="End date for date range (UTC, exclusive). "
             "Requires --start. Example: --end 2025-05-12"
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
    
    # Validate mutually exclusive date arguments
    if args.start and not args.end:
        parser.error("--start requires --end")
    if args.end and not args.start:
        parser.error("--end requires --start")
    if args.date and (args.start or args.end):
        parser.error("--date cannot be used with --start/--end")
    
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
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Rain Radar Data Collection - v%s", __version__)
        logger.info("=" * 80)
        
        # Determine time range
        start_time: Optional[datetime] = None
        end_time: Optional[datetime] = None
        mode: str = "current"
        
        if args.date:
            # Single date (historical)
            logger.info("Mode: Historical (single date)")
            start_time = parse_date(args.date, "--date")
            end_time = start_time + timedelta(days=1)
            mode = "historical"
            logger.info(f"Date: {args.date}")
            
        elif args.start and args.end:
            # Date range (historical)
            logger.info("Mode: Historical (date range)")
            start_time = parse_date(args.start, "--start")
            end_time = parse_date(args.end, "--end")
            mode = "historical"
            logger.info(f"Range: {args.start} to {args.end}")
            
            # Validate range
            validate_date_range(start_time, end_time)
            duration = (end_time - start_time).days
            logger.info(f"Duration: {duration} day(s)")
            
        else:
            # Default: last 24 hours (current)
            logger.info("Mode: Current (last 24 hours)")
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=24)
            mode = "current"
        
        # Log time range
        logger.info(f"Start time (UTC): {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time (UTC):   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Log processing options
        if args.force_refresh_pixels:
            logger.info("🔄 Pixel mappings: Force refresh from API")
        else:
            logger.info("💾 Pixel mappings: Use cached (if available)")
        
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
        if mode == "historical" and args.date:
            output_dir = f"outputs/rain_radar/historical/{args.date}/raw/"
            logger.info(f"Output location: {output_dir}")
        elif mode == "historical":
            logger.info("Output location: outputs/rain_radar/historical/YYYY-MM-DD/raw/")
        else:
            logger.info("Output location: outputs/rain_radar/raw/")
        
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