#!/usr/bin/env python3
"""
Rain Radar Alarm Detection Visualization Script

Creates interactive HTML dashboard showing when and where alarms were triggered
during a 24-hour period. Visualizes the detection results from check_alarm_timeline.py
with timeline, event analysis, and catchment statistics.

Features:
    - Interactive alarm timeline (when alarms occurred)
    - Alarm events analysis (duration, frequency)
    - Catchment statistics and ranking
    - Peak alarm statistics
    - Exportable results (CSV, charts)

Compatible with check_alarm_timeline.py output files:
    - alarm_timeline.csv: Status at every timestamp
    - alarm_events.csv: Distinct alarm events with duration
    - alarm_summary.txt: Text summary

Usage:
    # Visualize alarms for specific date
    python visualize_alarms.py --date 2026-01-22
    
    # Visualize today's alarms
    python visualize_alarms.py
    
    # Don't open browser automatically
    python visualize_alarms.py --date 2026-01-22 --no-open
    
    # Custom input/output directories
    python visualize_alarms.py --date 2026-01-22 --input outputs/rain_radar/alarms/
    
    # Verbose logging
    python visualize_alarms.py --date 2026-01-22 --log-level DEBUG

Output:
    outputs/rain_radar/YYYYMMDD-YYYYMMDD/visualizations/
    +-- alarms_dashboard.html         # Interactive dashboard
    +-- alarm_statistics.csv          # Summary statistics

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23
Version: 2.4.0 - Removed tkinter popup (GUI handles it)
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
import webbrowser
from datetime import datetime
from typing import Optional, Dict, Tuple

import pandas as pd

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths

__version__ = "2.4.0"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Visualize radar alarm detection results from check_alarm_timeline.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --date 2026-01-22         # Visualize specific date
  %(prog)s                           # Use today's date
  %(prog)s --date 2026-01-22 --no-open  # Don't open browser
  %(prog)s --date 2026-01-22 --log-level DEBUG

Input files (from check_alarm_timeline.py):
  - alarm_timeline.csv: Alarm status at every timestamp
  - alarm_events.csv: Distinct alarm events with duration

Output:
  Interactive HTML dashboard with alarm timeline and statistics
        """
    )
    
    # Date argument
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Date to visualize (default: today)"
    )
    
    # Input/output directories
    parser.add_argument(
        "--input",
        type=Path,
        help="Custom input directory with alarm_timeline.csv (default: auto-detect)"
    )
    
    parser.add_argument(
        "--output",
        type=Path,
        help="Custom output directory for visualizations (default: auto-detect)"
    )
    
    # Logging
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    # Browser option
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Do not open dashboard in browser after generation"
    )
    
    return parser.parse_args()


def load_alarm_data(
    input_dir: Path, 
    logger: logging.Logger
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load alarm data from check_alarm_timeline.py output files.
    
    Args:
        input_dir: Directory containing alarm_timeline.csv and alarm_events.csv
        logger: Logger instance
        
    Returns:
        Tuple of (timeline_df, events_df)
    """
    timeline_file = input_dir / "alarm_timeline.csv"
    events_file = input_dir / "alarm_events.csv"
    
    # Load timeline data
    if not timeline_file.exists():
        raise FileNotFoundError(
            f"Timeline file not found: {timeline_file}\n"
            f"Run check_alarm_timeline.py first to generate alarm data."
        )
    
    logger.info(f"Loading timeline data from: {timeline_file}")
    timeline_df = pd.read_csv(timeline_file)
    timeline_df["timestamp"] = pd.to_datetime(timeline_df["timestamp"])
    logger.info(f"  Loaded {len(timeline_df)} timeline records")
    
    # Load events data
    if events_file.exists():
        logger.info(f"Loading events data from: {events_file}")
        try:
            events_df = pd.read_csv(events_file)
            if len(events_df) > 0:
                if "start_time" in events_df.columns:
                    events_df["start_time"] = pd.to_datetime(events_df["start_time"])
                if "end_time" in events_df.columns:
                    events_df["end_time"] = pd.to_datetime(events_df["end_time"])
                logger.info(f"  Loaded {len(events_df)} alarm events")
            else:
                logger.info("  No alarm events recorded (file is empty)")
        except pd.errors.EmptyDataError:
            logger.info("  No alarm events recorded (file is empty)")
            events_df = pd.DataFrame()
    else:
        logger.warning(f"Events file not found: {events_file}")
        events_df = pd.DataFrame()
    
    return timeline_df, events_df


def generate_statistics(
    timeline_df: pd.DataFrame, 
    events_df: pd.DataFrame,
    logger: logging.Logger
) -> Dict:
    """
    Generate comprehensive statistics from alarm data.
    
    Args:
        timeline_df: DataFrame with timeline data
        events_df: DataFrame with events data
        logger: Logger instance
        
    Returns:
        Dictionary with statistics
    """
    stats = {}
    
    # Basic counts
    stats["total_records"] = len(timeline_df)
    stats["total_catchments"] = timeline_df["catchment_id"].nunique()
    stats["total_timestamps"] = timeline_df["timestamp"].nunique()
    
    # Alarm counts from timeline
    alarm_records = timeline_df[timeline_df["alarm"] == True]
    stats["total_alarm_records"] = len(alarm_records)
    stats["catchments_with_alarms"] = alarm_records["catchment_id"].nunique()
    
    # Time range
    if len(timeline_df) > 0:
        stats["period_start"] = timeline_df["timestamp"].min()
        stats["period_end"] = timeline_df["timestamp"].max()
        stats["period_hours"] = (stats["period_end"] - stats["period_start"]).total_seconds() / 3600
    
    # Event statistics
    if len(events_df) > 0:
        stats["total_events"] = len(events_df)
        stats["avg_event_duration_min"] = events_df["duration_minutes"].mean()
        stats["max_event_duration_min"] = events_df["duration_minutes"].max()
        stats["min_event_duration_min"] = events_df["duration_minutes"].min()
        
        # First and last alarm times
        stats["first_alarm"] = events_df["start_time"].min()
        stats["last_alarm"] = events_df["end_time"].max()
        
        # Peak proportion
        if "peak_proportion" in events_df.columns:
            stats["max_peak_proportion"] = events_df["peak_proportion"].max()
    else:
        stats["total_events"] = 0
    
    logger.info(f"Statistics generated:")
    logger.info(f"  Total catchments: {stats['total_catchments']}")
    logger.info(f"  Catchments with alarms: {stats['catchments_with_alarms']}")
    logger.info(f"  Total alarm events: {stats.get('total_events', 0)}")
    
    return stats


def get_top_catchments(
    timeline_df: pd.DataFrame, 
    events_df: pd.DataFrame,
    top_n: int = 15
) -> pd.DataFrame:
    """
    Get top catchments by alarm activity.
    
    Args:
        timeline_df: Timeline DataFrame
        events_df: Events DataFrame
        top_n: Number of top catchments to return
        
    Returns:
        DataFrame with top catchments and their statistics
    """
    # Count alarm timestamps per catchment
    alarm_records = timeline_df[timeline_df["alarm"] == True]
    
    if len(alarm_records) == 0:
        return pd.DataFrame()
    
    alarm_counts = alarm_records.groupby(
        ["catchment_id", "catchment_name"]
    ).size().reset_index(name="alarm_timestamps")
    
    # Count events per catchment
    if len(events_df) > 0:
        event_counts = events_df.groupby(
            ["catchment_id", "catchment_name"]
        ).agg({
            "event_id": "count",
            "duration_minutes": ["sum", "mean", "max"],
            "peak_proportion": "max"
        }).reset_index()
        
        # Flatten column names
        event_counts.columns = [
            "catchment_id", "catchment_name", "event_count",
            "total_duration_min", "avg_duration_min", "max_duration_min",
            "max_proportion"
        ]
        
        # Merge with alarm counts
        top_df = alarm_counts.merge(
            event_counts, 
            on=["catchment_id", "catchment_name"],
            how="left"
        )
    else:
        top_df = alarm_counts.copy()
        top_df["event_count"] = 0
        top_df["total_duration_min"] = 0
        top_df["avg_duration_min"] = 0
        top_df["max_duration_min"] = 0
        top_df["max_proportion"] = 0
    
    # Sort by alarm timestamps (most active first)
    top_df = top_df.sort_values("alarm_timestamps", ascending=False).head(top_n)
    
    return top_df


def get_longest_events(events_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Get longest alarm events.
    
    Args:
        events_df: Events DataFrame
        top_n: Number of events to return
        
    Returns:
        DataFrame with longest events
    """
    if len(events_df) == 0:
        return pd.DataFrame()
    
    return events_df.nlargest(top_n, "duration_minutes")[
        ["catchment_name", "start_time", "end_time", "duration_minutes", "peak_proportion"]
    ]


def create_html_dashboard(
    timeline_df: pd.DataFrame,
    events_df: pd.DataFrame,
    stats: Dict,
    output_dir: Path,
    logger: logging.Logger
) -> Path:
    """
    Create interactive HTML dashboard.
    
    Args:
        timeline_df: Timeline DataFrame
        events_df: Events DataFrame
        stats: Statistics dictionary
        output_dir: Output directory for HTML file
        logger: Logger instance
        
    Returns:
        Path to created HTML file
    """
    html_file = output_dir / "alarms_dashboard.html"
    logger.info(f"Creating HTML dashboard: {html_file}")
    
    # Get top catchments
    top_catchments = get_top_catchments(timeline_df, events_df)
    
    # Get longest events
    longest_events = get_longest_events(events_df)
    
    # Calculate percentages
    alarm_pct = (stats["catchments_with_alarms"] / stats["total_catchments"] * 100) if stats["total_catchments"] > 0 else 0
    
    # Format time values
    period_start_str = stats.get("period_start", "N/A")
    period_end_str = stats.get("period_end", "N/A")
    if isinstance(period_start_str, pd.Timestamp):
        period_start_str = period_start_str.strftime("%Y-%m-%d %H:%M")
    if isinstance(period_end_str, pd.Timestamp):
        period_end_str = period_end_str.strftime("%Y-%m-%d %H:%M")
    
    # Build HTML
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Radar Alarm Detection Dashboard</title>
    <style>
        * {{
            box-sizing: border-box;
        }}
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f0f2f5;
            color: #333;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            background: linear-gradient(135deg, #1a237e 0%, #283593 100%);
            color: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 28px;
        }}
        .header .subtitle {{
            opacity: 0.9;
            font-size: 14px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            text-align: center;
            transition: transform 0.2s;
        }}
        .stat-card:hover {{
            transform: translateY(-2px);
        }}
        .stat-card.alert {{
            background: linear-gradient(135deg, #d32f2f 0%, #c62828 100%);
            color: white;
        }}
        .stat-card.warning {{
            background: linear-gradient(135deg, #f57c00 0%, #ef6c00 100%);
            color: white;
        }}
        .stat-card.info {{
            background: linear-gradient(135deg, #1976d2 0%, #1565c0 100%);
            color: white;
        }}
        .stat-card.success {{
            background: linear-gradient(135deg, #388e3c 0%, #2e7d32 100%);
            color: white;
        }}
        .stat-value {{
            font-size: 36px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .stat-label {{
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .card {{
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            margin-bottom: 20px;
            overflow: hidden;
        }}
        .card-header {{
            background: #f8f9fa;
            padding: 15px 20px;
            border-bottom: 1px solid #e9ecef;
            font-weight: 600;
            font-size: 16px;
        }}
        .card-body {{
            padding: 20px;
            overflow-x: auto;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th {{
            background-color: #f8f9fa;
            padding: 12px 15px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #666;
            border-bottom: 2px solid #e9ecef;
        }}
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #f0f0f0;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
        }}
        .badge-danger {{
            background: #ffebee;
            color: #c62828;
        }}
        .badge-warning {{
            background: #fff3e0;
            color: #e65100;
        }}
        .badge-success {{
            background: #e8f5e9;
            color: #2e7d32;
        }}
        .progress-bar {{
            height: 8px;
            background: #e9ecef;
            border-radius: 4px;
            overflow: hidden;
        }}
        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #d32f2f, #f44336);
            border-radius: 4px;
        }}
        .two-columns {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        @media (max-width: 900px) {{
            .two-columns {{
                grid-template-columns: 1fr;
            }}
        }}
        .no-data {{
            text-align: center;
            padding: 40px;
            color: #666;
        }}
        .no-data .icon {{
            font-size: 48px;
            margin-bottom: 10px;
        }}
        footer {{
            margin-top: 30px;
            padding: 20px;
            text-align: center;
            font-size: 12px;
            color: #666;
        }}
        .time-info {{
            display: flex;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 15px;
            padding-top: 15px;
            border-top: 1px solid rgba(255,255,255,0.2);
        }}
        .time-info div {{
            font-size: 13px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📡 Radar Alarm Detection Dashboard</h1>
            <div class="subtitle">
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
                Moata AlertLab v{__version__}
            </div>
            <div class="time-info">
                <div>📅 Period: {period_start_str} → {period_end_str}</div>
                <div>⏱️ Duration: {stats.get('period_hours', 0):.1f} hours</div>
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card alert">
                <div class="stat-label">Total Alarm Events</div>
                <div class="stat-value">{stats.get('total_events', 0)}</div>
            </div>
            <div class="stat-card warning">
                <div class="stat-label">Catchments Affected</div>
                <div class="stat-value">{stats['catchments_with_alarms']}</div>
                <div class="stat-label" style="margin-top:5px">{alarm_pct:.1f}% of {stats['total_catchments']}</div>
            </div>
            <div class="stat-card info">
                <div class="stat-label">Alarm Timestamps</div>
                <div class="stat-value">{stats['total_alarm_records']:,}</div>
            </div>
            <div class="stat-card success">
                <div class="stat-label">Catchments OK</div>
                <div class="stat-value">{stats['total_catchments'] - stats['catchments_with_alarms']}</div>
            </div>
        </div>
"""
    
    # Event duration statistics (if events exist)
    if stats.get('total_events', 0) > 0:
        html_content += f"""
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Avg Event Duration</div>
                <div class="stat-value">{stats.get('avg_event_duration_min', 0):.0f}</div>
                <div class="stat-label">minutes</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Longest Event</div>
                <div class="stat-value">{stats.get('max_event_duration_min', 0):.0f}</div>
                <div class="stat-label">minutes</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Max Area Affected</div>
                <div class="stat-value">{stats.get('max_peak_proportion', 0) * 100:.1f}%</div>
                <div class="stat-label">of catchment</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Timestamps</div>
                <div class="stat-value">{stats['total_timestamps']:,}</div>
                <div class="stat-label">analyzed</div>
            </div>
        </div>
"""
    
    html_content += """
        <div class="two-columns">
            <div class="card">
                <div class="card-header">🏆 Top Catchments by Alarm Activity</div>
                <div class="card-body">
"""
    
    if len(top_catchments) > 0:
        max_timestamps = top_catchments["alarm_timestamps"].max()
        html_content += """
                    <table>
                        <thead>
                            <tr>
                                <th>Rank</th>
                                <th>Catchment</th>
                                <th>Events</th>
                                <th>Alarm Time</th>
                                <th>Activity</th>
                            </tr>
                        </thead>
                        <tbody>
"""
        for rank, (_, row) in enumerate(top_catchments.iterrows(), 1):
            pct = (row["alarm_timestamps"] / max_timestamps * 100) if max_timestamps > 0 else 0
            event_count = int(row.get("event_count", 0))
            badge_class = "badge-danger" if event_count >= 3 else "badge-warning" if event_count >= 1 else "badge-success"
            
            html_content += f"""
                            <tr>
                                <td><strong>{rank}</strong></td>
                                <td>{row["catchment_name"]}</td>
                                <td><span class="badge {badge_class}">{event_count}</span></td>
                                <td>{row["alarm_timestamps"]} timestamps</td>
                                <td>
                                    <div class="progress-bar">
                                        <div class="progress-fill" style="width: {pct}%"></div>
                                    </div>
                                </td>
                            </tr>
"""
        html_content += """
                        </tbody>
                    </table>
"""
    else:
        html_content += """
                    <div class="no-data">
                        <div class="icon">✅</div>
                        <div>No alarms detected during this period</div>
                    </div>
"""
    
    html_content += """
                </div>
            </div>
            
            <div class="card">
                <div class="card-header">⏱️ Longest Alarm Events</div>
                <div class="card-body">
"""
    
    if len(longest_events) > 0:
        html_content += """
                    <table>
                        <thead>
                            <tr>
                                <th>Catchment</th>
                                <th>Duration</th>
                                <th>Peak Area</th>
                                <th>Time</th>
                            </tr>
                        </thead>
                        <tbody>
"""
        for _, row in longest_events.iterrows():
            duration_str = f"{row['duration_minutes']} min"
            if row['duration_minutes'] >= 60:
                hours = row['duration_minutes'] / 60
                duration_str = f"{hours:.1f} hrs"
            
            peak_pct = row.get('peak_proportion', 0) * 100
            start_str = row['start_time'].strftime('%H:%M') if pd.notna(row['start_time']) else "N/A"
            end_str = row['end_time'].strftime('%H:%M') if pd.notna(row['end_time']) else "N/A"
            
            html_content += f"""
                            <tr>
                                <td>{row["catchment_name"]}</td>
                                <td><strong>{duration_str}</strong></td>
                                <td>{peak_pct:.1f}%</td>
                                <td>{start_str} - {end_str}</td>
                            </tr>
"""
        html_content += """
                        </tbody>
                    </table>
"""
    else:
        html_content += """
                    <div class="no-data">
                        <div class="icon">✅</div>
                        <div>No alarm events recorded</div>
                    </div>
"""
    
    html_content += """
                </div>
            </div>
        </div>
        
        <div class="card">
            <div class="card-header">ℹ️ About This Report</div>
            <div class="card-body">
                <p><strong>Alarm Detection Criteria:</strong></p>
                <ul>
                    <li>An alarm is triggered when ≥25% of a catchment's pixels exceed the ARI threshold</li>
                    <li>ARI (Average Recurrence Interval) threshold: 5 years</li>
                    <li>Alarms are checked at every timestamp using the LATEST window for each duration</li>
                </ul>
                <p><strong>Duration Windows Analyzed:</strong> 10min, 20min, 30min, 1hr, 2hr, 6hr, 12hr, 24hr</p>
                <p><strong>Data Source:</strong> MetService rain radar via Moata API</p>
            </div>
        </div>
        
        <footer>
            <p>Moata AlertLab - Radar Alarm Detection Dashboard</p>
            <p>Auckland Council Healthy Waters & Flood Resilience | COMPSCI 778 Internship Project</p>
        </footer>
    </div>
</body>
</html>
"""
    
    # Write HTML file
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    logger.info(f"✓ Dashboard created: {html_file}")
    
    return html_file


def save_statistics_csv(
    timeline_df: pd.DataFrame,
    events_df: pd.DataFrame,
    output_dir: Path,
    logger: logging.Logger
) -> Optional[Path]:
    """
    Save detailed statistics to CSV.
    
    Args:
        timeline_df: Timeline DataFrame
        events_df: Events DataFrame
        output_dir: Output directory
        logger: Logger instance
        
    Returns:
        Path to created file or None if no alarms
    """
    # Get catchment statistics
    top_catchments = get_top_catchments(timeline_df, events_df, top_n=999)
    
    if len(top_catchments) > 0:
        stats_file = output_dir / "alarm_statistics.csv"
        top_catchments.to_csv(stats_file, index=False)
        logger.info(f"✓ Statistics saved: {stats_file}")
        return stats_file
    else:
        logger.info("No alarm statistics to save (no alarms detected)")
        return None


def main() -> int:
    """Main entry point."""
    args = parse_args()
    logger = setup_script_logger(args.log_level, __name__)
    
    try:
        logger.info("=" * 80)
        logger.info(f"Radar Alarm Visualization - v{__version__}")
        logger.info("=" * 80)
        
        # Determine paths
        if args.date:
            paths = PipelinePaths.for_date(args.date)
            logger.info(f"Date: {args.date}")
        else:
            paths = PipelinePaths.for_today()
            logger.info("Date: Today")
        
        # Input directory (alarm results from check_alarm_timeline.py)
        if args.input:
            input_dir = args.input
        else:
            input_dir = paths.rain_radar_alarms_dir
        
        # Output directory (visualizations)
        if args.output:
            output_dir = args.output
        else:
            output_dir = paths.rain_radar_viz_dir
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Input directory:  {input_dir}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 80)
        logger.info("")
        
        # Load alarm data
        timeline_df, events_df = load_alarm_data(input_dir, logger)
        
        logger.info("")
        
        # Generate statistics
        stats = generate_statistics(timeline_df, events_df, logger)
        
        logger.info("")
        logger.info("Generating visualizations...")
        logger.info("")
        
        # Create HTML dashboard
        dashboard_path = create_html_dashboard(timeline_df, events_df, stats, output_dir, logger)
        
        # Save statistics CSV
        stats_path = save_statistics_csv(timeline_df, events_df, output_dir, logger)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Visualization completed successfully")
        logger.info("=" * 80)
        logger.info(f"Output directory: {output_dir}")
        logger.info("")
        logger.info("Files created:")
        logger.info(f"  • {dashboard_path.name}")
        if stats_path:
            logger.info(f"  • {stats_path.name}")
        logger.info("")
        
        # Prepare dashboard URL
        dashboard_url = dashboard_path.absolute().as_uri()
        
        # Open dashboard in browser (unless --no-open)
        if not args.no_open:
            # Open dashboard in browser
            logger.info(f"Opening dashboard in browser...")
            try:
                webbrowser.open(dashboard_url)
                logger.info(f"✓ Opened: {dashboard_url}")
            except Exception as e:
                logger.warning(f"Could not open browser automatically: {e}")
                logger.info(f"Please open manually: {dashboard_path.absolute()}")
        else:
            logger.info(f"Dashboard ready: {dashboard_path.absolute()}")
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        logger.error("")
        logger.error("Have you run check_alarm_timeline.py first?")
        logger.error("  python scripts/radar/check_alarm_timeline.py --date YYYY-MM-DD")
        return 1
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    sys.exit(main())