"""
Entry point for radar data collection.

Usage:
    python retrieve_rain_radar.py                    # Fetch last 24 hours
    python retrieve_rain_radar.py --date 2025-05-09  # Fetch specific date (historical)
    python retrieve_rain_radar.py --start 2025-05-09 --end 2025-05-10  # Date range
"""
import argparse
from datetime import datetime, timedelta, timezone

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.collect.runner import run_collect_radar


def main():
    parser = argparse.ArgumentParser(description="Collect rain radar data from Moata API")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Fetch data for specific date (full 24 hours UTC)",
    )
    parser.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Start date (UTC)",
    )
    parser.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        help="End date (UTC)",
    )
    parser.add_argument(
        "--force-refresh-pixels",
        action="store_true",
        help="Force rebuild pixel mappings from API",
    )
    
    args = parser.parse_args()
    
    setup_logging("INFO")
    
    # Determine time range
    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
        start_time = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=timezone.utc)
        end_time = start_time + timedelta(days=1)
    elif args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
        start_time = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0, tzinfo=timezone.utc)
    else:
        # Default: last 24 hours
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)
    
    run_collect_radar(
        start_time=start_time,
        end_time=end_time,
        force_refresh_pixels=args.force_refresh_pixels,
    )


if __name__ == "__main__":
    main()
