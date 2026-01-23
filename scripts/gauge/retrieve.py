#!/usr/bin/env python3
"""
Rain Gauge Data Collection Script

Collects rain gauge data from the Moata API for Auckland Council's rain monitoring network.
Fetches data for all active rain gauges and saves raw responses to outputs/rain_gauges/raw/.

Usage:
    # Collect last 24 hours (current data)
    gauge-retrieve
    
    # Collect specific date (historical)
    gauge-retrieve --date 2025-05-09
    
    # Collect date range
    gauge-retrieve --start 2025-05-09 --end 2025-05-10
    
    # Verbose logging
    gauge-retrieve --date 2025-05-09 --log-level DEBUG

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23 (FIXED: --date parameter now works correctly)
Version: 2.2.1
"""

import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from moata_pipeline.common.script_utils import (
    create_base_arg_parser,
    add_logging_args,
    setup_script_logger,
    print_script_header,
    print_script_footer,
    handle_keyboard_interrupt
)
from moata_pipeline.common.validation import validate_date_string
from moata_pipeline.collect.runner import run_collect_rain_gauges


# Version info
__version__ = "2.2.1"


def validate_date_range(start_time: datetime, end_time: datetime, logger) -> None:
    """
    Validate date range is logical.
    
    Args:
        start_time: Start datetime
        end_time: End datetime
        logger: Logger instance for warnings
        
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
        logger.warning(
            f"⚠️  Large date range: {duration} days. "
            f"This may take a long time and use significant disk space."
        )


def parse_args():
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = create_base_arg_parser(
        description="Collect rain gauge data from Moata API for Auckland rain monitoring network",
        script_name="retrieve.py",
        version=__version__,
        epilog="""
Examples:
  %(prog)s                                    # Collect last 24 hours (current)
  %(prog)s --date 2025-05-09                  # Collect specific date
  %(prog)s --start 2025-05-09 --end 2025-05-12  # Collect date range
  %(prog)s --date 2025-05-09 --log-level DEBUG  # Verbose logging

Notes:
  - All dates are in UTC timezone
  - Historical data available from 2024-01-01 onwards
  - Current data collection fetches last 24 hours
  - Collection duration: ~5-10 minutes depending on date range
  - Output: outputs/rain_gauges/YYYYMMDD-YYYYMMDD/raw/
        """
    )
    
    # Date options (mutually exclusive)
    date_group = parser.add_argument_group('Date Options')
    date_mutex = date_group.add_mutually_exclusive_group()
    
    date_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Fetch data for specific date (full 24 hours UTC). Example: --date 2025-05-09"
    )
    
    date_group.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Start date for date range (UTC, inclusive). Requires --end."
    )
    
    date_group.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        help="End date for date range (UTC, exclusive). Requires --start."
    )
    
    # Add logging arguments
    add_logging_args(parser)
    
    args = parser.parse_args()
    
    # Validate mutually exclusive date arguments
    if args.start and not args.end:
        parser.error("--start requires --end")
    if args.end and not args.start:
        parser.error("--end requires --start")
    if args.date and (args.start or args.end):
        parser.error("--date cannot be used with --start/--end")
    
    return args


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
        return e.code if e.code is not None else 0
    
    # Setup logging
    logger = setup_script_logger(args.log_level, __name__)
    
    try:
        # Print header
        print_script_header("Rain Gauge Data Collection", __version__, logger)
        
        # Determine time range
        start_time: Optional[datetime] = None
        end_time: Optional[datetime] = None
        mode: str = "current"
        
        if args.date:
            # ✅ FIXED: Parse tanggal yang diminta user
            logger.info("Mode: Historical (specific date)")
            logger.info(f"Requested date: {args.date}")
            
            # Parse dan validasi tanggal
            validated_date = validate_date_string(args.date, "%Y-%m-%d", "date")
            
            # Buat range 24 jam untuk tanggal tersebut
            # Contoh: 2025-05-09 → dari 2025-05-09 00:00:00 sampai 2025-05-10 00:00:00
            start_time = datetime(
                validated_date.year, validated_date.month, validated_date.day,
                0, 0, 0, tzinfo=timezone.utc
            )
            end_time = start_time + timedelta(hours=24)
            
            mode = "historical"
            logger.info(f"Date range: {start_time.date()} to {end_time.date()}")
            
        elif args.start and args.end:
            # Date range (historical) - validate and parse
            logger.info("Mode: Historical (date range)")
            validated_start = validate_date_string(args.start, "%Y-%m-%d", "start")
            validated_end = validate_date_string(args.end, "%Y-%m-%d", "end")
            start_time = datetime(
                validated_start.year, validated_start.month, validated_start.day,
                0, 0, 0, tzinfo=timezone.utc
            )
            end_time = datetime(
                validated_end.year, validated_end.month, validated_end.day,
                0, 0, 0, tzinfo=timezone.utc
            )
            mode = "historical"
            logger.info(f"Range: {args.start} to {args.end}")
            
            # Validate range
            validate_date_range(start_time, end_time, logger)
            duration = (end_time - start_time).days
            logger.info(f"Duration: {duration} day(s)")
            
        else:
            # Default: last 24 hours (current)
            logger.info("Mode: Current (last 24 hours)")
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=24)
            mode = "current"
        
        # Log time range
        if start_time and end_time:
            logger.info(f"Start time (UTC): {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"End time (UTC):   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("=" * 80)
        logger.info("")
        
        # Run collection
        logger.info("Starting rain gauge data collection...")
        run_collect_rain_gauges(
            start_time=start_time,
            end_time=end_time,
        )
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Rain gauge data collection completed successfully")
        logger.info("=" * 80)
        
        # Log output location (actual path is managed by runner with YYYYMMDD-YYYYMMDD format)
        from moata_pipeline.common.paths import PipelinePaths
        output_paths = PipelinePaths.for_date_range(start_time, end_time)
        logger.info(f"Output location: {output_paths.rain_gauges_raw_dir}")
        
        print_script_footer(logger, success=True)
        return 0
        
    except KeyboardInterrupt:
        return handle_keyboard_interrupt(logger)

    except ValueError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Validation Error")
        logger.error("=" * 80)
        logger.error(str(e))
        logger.error("")
        logger.error("Run with --help for usage information.")
        print_script_footer(logger, success=False)
        return 1
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Unexpected Error")
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
        print_script_footer(logger, success=False)
        return 1


if __name__ == "__main__":
    sys.exit(main())