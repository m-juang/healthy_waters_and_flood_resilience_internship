#!/usr/bin/env python3
"""
Radar Alarm Timeline Checker Script

Check radar alarms across entire timeline (every timestamp) for a date period.
Implements Sam's requirement: check LATEST window at each timestamp.

This is different from:
- check_radar_alarms.py: Checks only ONE specific timestamp
- validate.py: Checks MAXIMUM ARI across entire period

This script checks alarm status at EVERY timestamp to show how alarms
evolve over time (alarm events, duration, patterns).

OPTIMIZED VERSION (v2.3.0):
- Uses THREADED processing by default for 3-5x faster execution
- ThreadPoolExecutor is Windows-safe (no deadlocks like multiprocessing)
- 232 catchments processed in ~30-45 minutes instead of 3+ hours

Usage:
    # Check last 24 hours (threaded by default)
    python check_alarm_timeline.py
    
    # Check specific date
    python check_alarm_timeline.py --date 2026-01-22
    
    # Control number of threads
    python check_alarm_timeline.py --date 2026-01-22 --workers 8
    
    # Force sequential processing (slower but uses less memory)
    python check_alarm_timeline.py --date 2026-01-22 --sequential

Output:
    - alarm_timeline.csv: Alarm status at every timestamp for all catchments
    - alarm_events.csv: Separate alarm events with start/end/duration
    - alarm_summary.txt: Human-readable summary with statistics

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23
Version: 2.3.0 - ThreadPoolExecutor for faster Windows-safe processing
"""

import sys
import time
import os
from pathlib import Path

# Setup Project Root
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.alarms.radar_alarm_checker import RadarAlarmChecker


__version__ = "2.3.0"


def parse_args():
    """Parse command-line arguments."""
    default_workers = min(8, os.cpu_count() or 4)
    
    parser = argparse.ArgumentParser(
        description="Check radar alarm timeline - THREADED VERSION (Windows-safe)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  %(prog)s                                  # Check last 24 hours (threaded)
  %(prog)s --date 2026-01-22                # Historical date
  %(prog)s --workers 8                      # Use 8 threads
  %(prog)s --sequential                     # Force sequential (slower)

Output:
  - alarm_timeline.csv: Status at every timestamp
  - alarm_events.csv: Distinct alarm events  
  - alarm_summary.txt: Summary statistics

Performance (232 catchments):
  - Sequential: ~3+ hours
  - Threaded ({default_workers} workers): ~30-45 minutes
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
        help="Check current data (last 24 hours) - default"
    )
    
    # Threshold parameters
    threshold_group = parser.add_argument_group('Alarm Thresholds')
    threshold_group.add_argument(
        "--ari-threshold",
        type=float,
        default=5.0,
        help="ARI threshold in years (default: 5.0)"
    )
    
    threshold_group.add_argument(
        "--area-threshold",
        type=float,
        default=0.25,
        help="Area threshold as proportion 0-1 (default: 0.25 = 25%%)"
    )
    
    # Performance options
    perf_group = parser.add_argument_group('Performance')
    perf_group.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help=f"Number of threads for parallel processing (default: {default_workers})"
    )
    
    perf_group.add_argument(
        "--sequential",
        action="store_true",
        help="Use sequential processing (slower but uses less memory)"
    )
    
    # Visualization option
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate HTML timeline visualization"
    )
    
    # Logging
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    return parser.parse_args()


def identify_alarm_events(timeline_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify distinct alarm events from timeline.
    An alarm event is a continuous period where alarm=True for a catchment.
    """
    if timeline_df.empty:
        return pd.DataFrame()

    events = []
    
    # Group by catchment
    for (catchment_id, catchment_name), group in timeline_df.groupby(['catchment_id', 'catchment_name']):
        group = group.sort_values('timestamp')
        
        # Identify alarm periods (where alarm=True)
        alarm_series = group['alarm'].astype(int)
        
        # Find transitions (alarm starts/ends)
        transitions = alarm_series.diff()
        
        # Start of alarm event: transition from 0 to 1
        starts = group[transitions == 1].index
        
        # End of alarm event: transition from 1 to 0
        ends = group[transitions == -1].index
        
        # Handle edge cases
        if len(group) > 0 and alarm_series.iloc[0] == 1:
            starts = starts.insert(0, group.index[0])
        
        if len(group) > 0 and alarm_series.iloc[-1] == 1:
            ends = ends.insert(len(ends), group.index[-1])
        
        # Match starts with ends
        for event_num, (start_idx, end_idx) in enumerate(zip(starts, ends), start=1):
            start_row = group.loc[start_idx]
            end_row = group.loc[end_idx]
            
            event_data = group.loc[start_idx:end_idx]
            
            duration = (end_row['timestamp'] - start_row['timestamp']).total_seconds() / 60
            peak_prop = event_data['proportion'].max()
            
            events.append({
                'catchment_id': catchment_id,
                'catchment_name': catchment_name,
                'event_id': event_num,
                'start_time': start_row['timestamp'],
                'end_time': end_row['timestamp'],
                'duration_minutes': int(duration),
                'peak_proportion': peak_prop,
                'timestamps_count': len(event_data)
            })
    
    return pd.DataFrame(events)


def generate_summary_report(timeline_df: pd.DataFrame, events_df: pd.DataFrame, 
                           ari_threshold: float, area_threshold: float,
                           processing_time: float = 0,
                           processing_mode: str = "Unknown") -> str:
    """Generate human-readable summary report."""
    
    total_catchments = timeline_df['catchment_id'].nunique() if not timeline_df.empty else 0
    total_timestamps = timeline_df['timestamp'].nunique() if not timeline_df.empty else 0
    
    catchments_with_alarms = events_df['catchment_id'].nunique() if not events_df.empty else 0
    total_events = len(events_df)
    
    if not timeline_df.empty:
        period_start = timeline_df['timestamp'].min()
        period_end = timeline_df['timestamp'].max()
        period_duration = (period_end - period_start).total_seconds() / 3600
    else:
        period_start = datetime.now()
        period_end = datetime.now()
        period_duration = 0
    
    report = []
    report.append("=" * 80)
    report.append("RADAR ALARM TIMELINE SUMMARY REPORT")
    report.append("=" * 80)
    report.append("")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Script Version: {__version__}")
    report.append(f"Processing Mode: {processing_mode}")
    if processing_time > 0:
        report.append(f"Processing Time: {processing_time:.1f} seconds ({processing_time/60:.1f} minutes)")
    report.append("")
    
    report.append("PERIOD ANALYZED")
    report.append("-" * 80)
    report.append(f"Start:     {period_start.strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"End:       {period_end.strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Duration:  {period_duration:.1f} hours ({period_duration/24:.1f} days)")
    report.append(f"Timestamps: {total_timestamps}")
    report.append("")
    
    report.append("THRESHOLDS")
    report.append("-" * 80)
    report.append(f"ARI Threshold:  {ari_threshold} years")
    report.append(f"Area Threshold: {area_threshold*100}% of catchment pixels")
    report.append("")
    
    report.append("CATCHMENT SUMMARY")
    report.append("-" * 80)
    report.append(f"Total Catchments:          {total_catchments}")
    if total_catchments > 0:
        report.append(f"Catchments with Alarms:    {catchments_with_alarms} ({catchments_with_alarms/total_catchments*100:.1f}%)")
        report.append(f"Catchments Always OK:      {total_catchments - catchments_with_alarms}")
    report.append("")
    
    report.append("ALARM EVENT SUMMARY")
    report.append("-" * 80)
    report.append(f"Total Alarm Events:        {total_events}")
    report.append("")
    
    if total_events > 0:
        avg_duration = events_df['duration_minutes'].mean()
        max_duration = events_df['duration_minutes'].max()
        min_duration = events_df['duration_minutes'].min()
        
        report.append(f"Average Event Duration:    {avg_duration:.0f} minutes ({avg_duration/60:.1f} hours)")
        report.append(f"Longest Event:             {max_duration:.0f} minutes ({max_duration/60:.1f} hours)")
        report.append(f"Shortest Event:            {min_duration:.0f} minutes")
        report.append("")
        
        report.append("TOP 10 CATCHMENTS BY ALARM EVENTS")
        report.append("-" * 80)
        top_catchments = events_df.groupby(['catchment_id', 'catchment_name']).size().reset_index(name='event_count')
        top_catchments = top_catchments.sort_values('event_count', ascending=False).head(10)
        
        for i, row in top_catchments.iterrows():
            report.append(f"  {row['catchment_name']:<50} {row['event_count']:>3} events")
        report.append("")
        
        report.append("LONGEST ALARM EVENTS")
        report.append("-" * 80)
        longest_events = events_df.nlargest(10, 'duration_minutes')
        
        for i, row in longest_events.iterrows():
            report.append(f"  {row['catchment_name']:<40} {row['duration_minutes']:>5} min  "
                         f"{row['start_time'].strftime('%Y-%m-%d %H:%M')} - {row['end_time'].strftime('%H:%M')}")
        report.append("")
    else:
        report.append("No alarm events detected during this period.")
        report.append("")
    
    report.append("=" * 80)
    report.append("END OF REPORT")
    report.append("=" * 80)
    
    return "\n".join(report)


def process_sequential(
    checker: RadarAlarmChecker,
    radar_files: list,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Process catchments sequentially (slower but uses less memory).
    """
    all_timelines = []
    
    for i, filepath in enumerate(radar_files, start=1):
        logger.info(f"[{i}/{len(radar_files)}] Processing {filepath.name}")
        
        try:
            parts = filepath.stem.split("_", 1)
            catchment_id = int(parts[0]) if parts[0].isdigit() else None
            catchment_name = parts[1] if len(parts) > 1 else filepath.stem
            
            timeline_df = checker.check_catchment_timeline(
                catchment_csv=filepath,
                catchment_id=catchment_id,
                catchment_name=catchment_name,
            )
            
            all_timelines.append(timeline_df)
            
            alarm_count = timeline_df['alarm'].sum()
            
            if alarm_count > 0:
                logger.info(f"  🚨 {alarm_count} timestamps with alarms")
            else:
                logger.info(f"  ✓ No alarms")
            
        except Exception as e:
            logger.error(f"  ❌ Error: {e}")
            continue
    
    return pd.concat(all_timelines, ignore_index=True) if all_timelines else pd.DataFrame()


def main():
    """Main execution."""
    args = parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    logger = setup_script_logger(log_level=log_level, script_name="check_alarm_timeline")
    
    logger.info("=" * 80)
    logger.info("RADAR ALARM TIMELINE CHECKER")
    logger.info(f"Version {__version__}")
    logger.info("=" * 80)
    logger.info("")
    
    # Determine data directory based on date
    if args.date:
        paths = PipelinePaths.for_date(args.date)
        data_type = f"Historical ({args.date})"
    else:
        paths = PipelinePaths.for_today()
        data_type = "Current (Today)"
    
    data_dir = paths.rain_radar_data_dir
    output_dir = paths.rain_radar_alarms_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine processing mode
    if args.sequential:
        processing_mode = "Sequential"
    else:
        processing_mode = f"Threaded ({args.workers} workers)"
    
    logger.info(f"Data type: {data_type}")
    logger.info(f"Input directory: {data_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"ARI threshold: {args.ari_threshold} years")
    logger.info(f"Area threshold: {args.area_threshold * 100}%")
    logger.info(f"Processing mode: {processing_mode}")
    logger.info("=" * 80)
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
    
    # Start timing
    start_time = time.time()
    
    # Process catchments
    if args.sequential:
        logger.info("Using SEQUENTIAL processing")
        logger.info("(Use without --sequential for faster threaded processing)")
        logger.info("")
        full_timeline = process_sequential(checker, radar_files, logger)
    else:
        logger.info(f"Using THREADED processing with {args.workers} workers")
        logger.info("This is ~3-5x faster than sequential processing")
        logger.info("")
        full_timeline = checker.check_multiple_catchments_threaded(
            catchment_files=radar_files,
            max_workers=args.workers,
        )
    
    # Calculate processing time
    processing_time = time.time() - start_time
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("COMBINING RESULTS")
    logger.info("=" * 80)
    logger.info("")
    logger.info(f"Processing completed in {processing_time:.1f} seconds ({processing_time/60:.1f} minutes)")
    
    if full_timeline.empty:
        logger.error("No timeline data was generated!")
        return 1
    
    # Save timeline
    timeline_path = output_dir / "alarm_timeline.csv"
    full_timeline.to_csv(timeline_path, index=False)
    logger.info(f"✓ Saved timeline: {timeline_path}")
    logger.info(f"  Total records: {len(full_timeline)}")
    
    # Identify alarm events
    logger.info("")
    logger.info("Identifying distinct alarm events...")
    events_df = identify_alarm_events(full_timeline)
    
    events_path = output_dir / "alarm_events.csv"
    events_df.to_csv(events_path, index=False)
    logger.info(f"✓ Saved events: {events_path}")
    logger.info(f"  Total events: {len(events_df)}")
    
    # Generate summary report
    logger.info("")
    logger.info("Generating summary report...")
    summary = generate_summary_report(
        full_timeline, events_df, 
        args.ari_threshold, args.area_threshold,
        processing_time, processing_mode
    )
    
    summary_path = output_dir / "alarm_summary.txt"
    summary_path.write_text(summary, encoding='utf-8')
    logger.info(f"✓ Saved summary: {summary_path}")
    
    # Print summary to console
    logger.info("")
    logger.info(summary)
    
    # Generate visualization if requested
    if args.visualize:
        logger.info("")
        logger.info("=" * 80)
        logger.info("GENERATING VISUALIZATION")
        logger.info("=" * 80)
        logger.info("")
        logger.info("⚠️  Visualization not yet implemented")
        logger.info("Coming soon: Interactive HTML timeline with charts")
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("✅ ALARM TIMELINE CHECK COMPLETE")
    logger.info("=" * 80)
    logger.info("")
    logger.info(f"Processing time: {processing_time:.1f} seconds ({processing_time/60:.1f} minutes)")
    logger.info(f"Output files:")
    logger.info(f"  • {timeline_path}")
    logger.info(f"  • {events_path}")
    logger.info(f"  • {summary_path}")
    logger.info("")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())