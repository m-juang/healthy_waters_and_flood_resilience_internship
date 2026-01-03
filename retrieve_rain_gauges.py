#!/usr/bin/env python3
"""
Rain Gauge Data Collection Script

Collects rain gauge data from the Moata API for Auckland Council's rain monitoring network.
Fetches data for all active rain gauges and saves raw responses to outputs/rain_gauges/raw/.

Usage:
    # Collect last 24 hours (current data)
    python retrieve_rain_gauges.py
    
    # Collect specific date (historical)
    python retrieve_rain_gauges.py --date 2025-05-09
    
    # Collect date range
    python retrieve_rain_gauges.py --start 2025-05-09 --end 2025-05-10
    
    # Verbose logging
    python retrieve_rain_gauges.py --date 2025-05-09 --log-level DEBUG

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-03
Version: 2.0.0
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone  # ← MODIFIED: added timedelta, timezone
from pathlib import Path
from typing import Optional  # ← ADDED
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.collect.runner import run_collect_rain_gauges


# Version info
__version__ = "2.0.0"  # ← MODIFIED: updated version


# ============================================================================
# ← ADDED: New helper functions (parse_date, validate_date_range)
# ============================================================================

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


# ============================================================================
# ← MODIFIED: parse_args() - added date arguments
# ============================================================================

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Collect rain gauge data from Moata API for Auckland rain monitoring network",
        epilog="""
Examples:
  # Collect last 24 hours (current data)
  %(prog)s
  
  # Collect specific historical date
  %(prog)s --date 2025-05-09
  
  # Collect date range (multiple days)
  %(prog)s --start 2025-05-09 --end 2025-05-12
  
  # Verbose logging for debugging
  %(prog)s --date 2025-05-09 --log-level DEBUG
  
  # Combine options
  %(prog)s --start 2025-05-01 --end 2025-05-07 --log-level INFO

Notes:
  - All dates are in UTC timezone
  - Historical data available from 2024-01-01 onwards
  - Current data collection fetches last 24 hours
  - Collection duration: ~5-10 minutes depending on date range
  - Output location: outputs/rain_gauges/raw/ or outputs/rain_gauges/historical/YYYY-MM-DD/raw/
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # ← ADDED: Date options (mutually exclusive)
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
    
    # Logging options (EXISTING - kept as is)
    log_group = parser.add_argument_group('Logging Options')
    
    log_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO). "
             "Use DEBUG for verbose output."
    )
    
    # Metadata (EXISTING - kept as is)
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    args = parser.parse_args()
    
    # ← ADDED: Validate mutually exclusive date arguments
    if args.start and not args.end:
        parser.error("--start requires --end")
    if args.end and not args.start:
        parser.error("--end requires --start")
    if args.date and (args.start or args.end):
        parser.error("--date cannot be used with --start/--end")
    
    return args


# ============================================================================
# ← MODIFIED: main() - added time range handling
# ============================================================================

def main() -> int:
    """
    Main entry point for rain gauge data collection.
    
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
        logger.info("Rain Gauge Data Collection - v%s", __version__)
        logger.info("=" * 80)
        
        # ← ADDED: Determine time range
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
        
        # ← ADDED: Log time range
        if start_time and end_time:
            logger.info(f"Start time (UTC): {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"End time (UTC):   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("=" * 80)
        logger.info("")
        
        # ← MODIFIED: Run collection with time parameters
        logger.info("Starting rain gauge data collection...")
        
        run_collect_rain_gauges(
            start_time=start_time,  # ← ADDED
            end_time=end_time,      # ← ADDED
        )
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Rain gauge data collection completed successfully")
        logger.info("=" * 80)
        
        # ← ADDED: Log output location
        if mode == "historical" and args.date:
            output_dir = f"outputs/rain_gauges/historical/{args.date}/raw/"
            logger.info(f"Output location: {output_dir}")
        elif mode == "historical":
            logger.info("Output location: outputs/rain_gauges/historical/YYYY-MM-DD/raw/")
        else:
            logger.info("Output location: outputs/rain_gauges/raw/")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("⚠️  Collection interrupted by user (Ctrl+C)")
        logger.warning("=" * 80)
        logger.warning("Partial data may have been saved.")
        logger.warning("You can resume by running the script again.")
        return 130  # ← EXISTING: kept as is
        
    # ← ADDED: ValueError handling
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
        logger.error("3. Check disk space")
        logger.error("4. Try with --log-level DEBUG for more details")
        logger.error("5. Check if date is valid (historical data from 2024-01-01)")
        return 1


if __name__ == "__main__":
    sys.exit(main())