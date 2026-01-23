#!/usr/bin/env python3
"""
Real-time Radar Alarm Checker Script

Check current radar data against alarm thresholds.
Implements Sam's requirement: check only LATEST window for each duration.

Usage:
    # Check most recent timestamp in data
    python check_radar_alarms.py
    
    # Check specific timestamp
    python check_radar_alarms.py --time "2026-01-14 14:00:00"
    
    # Custom thresholds
    python check_radar_alarms.py --ari-threshold 10.0 --area-threshold 0.25
    
    # Historical date
    python check_radar_alarms.py --date 2025-05-09

Output:
    - alarm_status.csv: Current alarm status per catchment
    - triggered_alarms.csv: Only catchments with active alarms
    - alarm_report.txt: Human-readable summary

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-16
Version: 1.0.0
"""

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
from datetime import datetime

import pandas as pd

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.alarms.radar_alarm_checker import RadarAlarmChecker


__version__ = "1.0.0"


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Check radar alarms at specific timestamp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                  # Check most recent data
  %(prog)s --time "2026-01-14 14:00:00"     # Specific time
  %(prog)s --date 2025-05-09                # Historical date
  %(prog)s --current                        # Current (last 24h) explicitly

Output:
  - alarm_status.csv: All catchments with alarm status
  - triggered_alarms.csv: Only active alarms
  - alarm_report.txt: Summary report

Key Concept:
  This checks alarm status at a SPECIFIC timestamp using LATEST windows only.
  Different from analysis which finds MAXIMUM across entire period.
        """
    )
    
    # Input options
    source_group = parser.add_argument_group('Data Source')
    source_mutex = source_group.add_mutually_exclusive_group()
    
    source_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Check historical data for specific date"
    )
    
    source_mutex.add_argument(
        "--current",
        action="store_true",
        help="Check current (last 24h) data"
    )
    
    # Time option
    parser.add_argument(
        "--time",
        metavar="YYYY-MM-DD HH:MM:SS",
        help="Specific timestamp to check (default: most recent in data)"
    )
    
    # Threshold options
    parser.add_argument(
        "--ari-threshold",
        type=float,
        default=5.0,
        help="ARI threshold in years (default: 5.0)"
    )
    
    parser.add_argument(
        "--area-threshold",
        type=float,
        default=0.25,
        help="Area threshold (default: 0.25 = 25%%)"
    )
    
    # Output option
    parser.add_argument(
        "--output-dir",
        help="Output directory (auto-determined if not specified)"
    )
    
    # Logging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    logger = setup_script_logger(args.log_level, __name__)
    
    # Determine data directory and output directory using PipelinePaths
    if args.date:
        paths = PipelinePaths.for_date(args.date)
        data_dir = paths.rain_radar_data_dir
        output_dir = paths.rain_radar_alarms_dir
    elif args.current:
        paths = PipelinePaths.for_today()
        data_dir = paths.rain_radar_data_dir
        output_dir = paths.rain_radar_alarms_dir
    else:
        # Auto-detect (prefer most recent date)
        # Format: outputs/rain_radar/YYYYMMDD-YYYYMMDD/raw/radar_data
        historical_base = Path("outputs/rain_radar")
        historical_dirs = sorted(
            [d for d in historical_base.glob("*-*/raw/radar_data") if d.exists()],
            key=lambda p: p.parent.parent.name,  # Sort by date folder name
            reverse=True
        )
        
        if historical_dirs:
            data_dir = historical_dirs[0]
            # Extract date range from path
            date_range_folder = data_dir.parent.parent.name  # e.g., "20260120-20260121"
            
            # Parse end date from folder name for PipelinePaths
            if "-" in date_range_folder and len(date_range_folder) == 17:
                end_date_str = date_range_folder.split("-")[1]  # YYYYMMDD
                date_str = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                paths = PipelinePaths.for_date(date_str)
            else:
                paths = PipelinePaths.for_today()
            output_dir = paths.rain_radar_alarms_dir
        else:
            # Fallback to current/today
            paths = PipelinePaths.for_today()
            data_dir = paths.rain_radar_data_dir
            output_dir = paths.rain_radar_alarms_dir
    
    # Override output_dir if provided
    if args.output_dir:
        output_dir = Path(args.output_dir)
    
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        logger.error("")
        logger.error("Have you run data collection first?")
        logger.error(f"  python scripts/radar/retrieve.py" + (f" --date {args.date}" if args.date else ""))
        return 1
    
    logger.info("=" * 80)
    logger.info("Radar Alarm Checker - v%s", __version__)
    logger.info("=" * 80)
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"ARI threshold: {args.ari_threshold} years")
    logger.info(f"Area threshold: {args.area_threshold * 100}%")
    logger.info("=" * 80)
    logger.info("")
    
    # Parse check time if provided
    check_time = None
    if args.time:
        try:
            check_time = datetime.fromisoformat(args.time)
            logger.info(f"Checking at: {check_time}")
        except ValueError as e:
            logger.error(f"Invalid time format: {e}")
            return 1
    else:
        logger.info("Checking at: Most recent timestamp in data")
    
    logger.info("")
    
    # Initialize checker
    try:
        checker = RadarAlarmChecker(
            tp108_path=Path("data/inputs/tp108_stats.csv"),
            ari_threshold=args.ari_threshold,
            area_threshold=args.area_threshold,
        )
    except Exception as e:
        logger.error(f"Failed to initialize alarm checker: {e}")
        return 1
    
    # Get radar files
    radar_files = list(data_dir.glob("*.csv"))
    logger.info(f"Found {len(radar_files)} catchment files")
    logger.info("")
    
    if len(radar_files) == 0:
        logger.error("No radar data files found!")
        logger.error("Run data collection first:")
        logger.error(f"  python scripts/radar/retrieve.py" + (f" --date {args.date}" if args.date else ""))
        return 1
    
    # Check each catchment
    results = []
    
    for i, filepath in enumerate(radar_files, start=1):
        logger.info(f"[{i}/{len(radar_files)}] Checking {filepath.name}")
        
        try:
            # Load data
            df = pd.read_csv(filepath)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Use provided time or most recent
            ts = check_time if check_time else df["timestamp"].max()
            
            # Extract catchment info
            parts = filepath.stem.split("_", 1)
            catchment_id = int(parts[0]) if parts[0].isdigit() else None
            catchment_name = parts[1] if len(parts) > 1 else filepath.stem
            
            # Check alarm
            result = checker.check_catchment_at_time(
                catchment_df=df,
                check_time=ts,
                catchment_id=catchment_id,
                catchment_name=catchment_name,
            )
            
            results.append(result)
            
            if result["alarm"]:
                logger.warning(f"  🚨 ALARM! {result['proportion']*100:.1f}% pixels exceed")
            else:
                logger.info(f"  ✓ No alarm ({result['proportion']*100:.1f}% pixels exceed)")
                
        except Exception as e:
            logger.error(f"  Failed: {e}")
            continue
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("Saving results...")
    logger.info("=" * 80)
    
    # Save results
    output_dir.mkdir(parents=True, exist_ok=True)
    
    df = pd.DataFrame(results)
    
    # Remove exceeding_details column for CSV (too detailed)
    if "exceeding_details" in df.columns:
        df = df.drop(columns=["exceeding_details"])
    
    df = df.sort_values(["alarm", "proportion"], ascending=[False, False])
    
    # All status
    status_path = output_dir / "alarm_status.csv"
    df.to_csv(status_path, index=False)
    logger.info(f"✓ Saved alarm status: {status_path}")
    
    # Triggered only
    triggered = df[df["alarm"] == True]
    triggered_path = output_dir / "triggered_alarms.csv"
    triggered.to_csv(triggered_path, index=False)
    logger.info(f"✓ Saved triggered alarms: {triggered_path}")
    
    # Summary report
    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("RADAR ALARM CHECK REPORT")
    report_lines.append("=" * 70)
    report_lines.append(f"Check time: {check_time or 'Most recent in data'}")
    report_lines.append(f"ARI threshold: {args.ari_threshold} years")
    report_lines.append(f"Area threshold: {args.area_threshold*100:.0f}% of catchment")
    report_lines.append("")
    report_lines.append(f"Total catchments: {len(df)}")
    report_lines.append(f"Alarms triggered: {len(triggered)}")
    report_lines.append(f"Status OK: {len(df) - len(triggered)}")
    report_lines.append("")
    
    if len(triggered) > 0:
        report_lines.append("ACTIVE ALARMS:")
        report_lines.append("-" * 70)
        for _, row in triggered.iterrows():
            report_lines.append(f"  {row['catchment_name']}")
            report_lines.append(
                f"    Pixels: {row['pixels_exceeding']}/{row['pixels_total']} "
                f"({row['proportion']*100:.1f}%)"
            )
    else:
        report_lines.append("NO ALARMS TRIGGERED")
        report_lines.append("-" * 70)
        report_lines.append("All catchments below area threshold.")
    
    report_lines.append("")
    report_lines.append("=" * 70)
    
    report = "\n".join(report_lines)
    report_path = output_dir / "alarm_report.txt"
    report_path.write_text(report, encoding="utf-8")
    logger.info(f"✓ Saved alarm report: {report_path}")
    
    # Print summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("✓ ALARM CHECK COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total catchments: {len(df)}")
    logger.info(f"Alarms triggered: {len(triggered)}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("=" * 80)
    
    if len(triggered) > 0:
        logger.info("")
        logger.warning("🚨 ACTIVE ALARMS:")
        for _, row in triggered.iterrows():
            logger.warning(f"  - {row['catchment_name']}: {row['proportion']*100:.1f}% pixels exceed")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())