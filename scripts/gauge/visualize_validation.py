#!/usr/bin/env python3
"""
Rain Gauge ARI Alarm Validation Visualization Script

Creates charts and HTML dashboard showing validation status, exceedances, and statistics
for rain gauge ARI alarm validation results.

Usage:
    python visualize_ari_alarms_rain_gauges.py [options]

Examples:
    # Auto-detect latest validation file
    python visualize_ari_alarms_rain_gauges.py
    
    # Use specific date (will search YYYYMMDD-YYYYMMDD+1 folder)
    python visualize_ari_alarms_rain_gauges.py --date 2026-01-21
    python visualize_ari_alarms_rain_gauges.py --date 20260121
    
    # Use custom input CSV
    python visualize_ari_alarms_rain_gauges.py --input outputs/custom/validation.csv
    
    # Specify custom output directory
    python visualize_ari_alarms_rain_gauges.py --output outputs/custom_viz/
    
    # Enable debug logging
    python visualize_ari_alarms_rain_gauges.py --log-level DEBUG

Output:
    - validation_summary.png: Pie chart of validation status
    - top_exceedances.png: Bar chart of top ARI exceedances
    - validation_dashboard.html: Interactive HTML dashboard
    - validation_stats.csv: Statistics CSV file

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23
"""

import sys
from pathlib import Path
# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths

# Default paths
paths = PipelinePaths()
DEFAULT_OUT_DIR = paths.rain_gauges_validation_dir


def parse_date_string(date_str: str) -> datetime:
    """
    Parse date string in various formats.
    
    Args:
        date_str: Date string (YYYY-MM-DD, YYYYMMDD, etc.)
        
    Returns:
        datetime object
        
    Examples:
        >>> parse_date_string("2026-01-21")
        datetime(2026, 1, 21)
        >>> parse_date_string("20260121")
        datetime(2026, 1, 21)
    """
    # Remove spaces and dashes
    clean_str = date_str.replace("-", "").replace(" ", "").strip()
    
    # Try YYYYMMDD format
    if len(clean_str) == 8 and clean_str.isdigit():
        return datetime.strptime(clean_str, "%Y%m%d")
    
    # Try with dashes
    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    raise ValueError(
        f"Unable to parse date: '{date_str}'\n"
        "Supported formats: YYYY-MM-DD, YYYYMMDD, DD-MM-YYYY, etc."
    )


def build_date_folder_name(date: datetime) -> str:
    """
    Build folder name from date (YYYYMMDD-YYYYMMDD+1).
    
    Args:
        date: Start date
        
    Returns:
        Folder name string
        
    Example:
        >>> build_date_folder_name(datetime(2026, 1, 21))
        '20260121-20260122'
    """
    next_day = date + timedelta(days=1)
    start = date.strftime("%Y%m%d")
    end = next_day.strftime("%Y%m%d")
    return f"{start}-{end}"


def find_validation_file_by_date(date_str: str, logger) -> Path:
    """
    Find validation file based on date input.
    
    Args:
        date_str: Date string to search for
        logger: Logger instance
        
    Returns:
        Path to validation file
    """
    # Use PipelinePaths.for_date() to get the correct path for the date
    date_paths = PipelinePaths.for_date(date_str)
    validation_path = date_paths.rain_gauges_validation_dir / "ari_alarm_validation.csv"
    
    logger.info(f"Looking for validation file: {validation_path}")
    
    if not validation_path.exists():
        raise FileNotFoundError(
            f"Validation file not found: {validation_path}\n"
            f"Please ensure validate.py was run for date: {date_str}"
        )
    
    return validation_path


def find_latest_validation_file(logger) -> Path:
    """
    Find the most recent validation CSV file.
    
    Returns:
        Path to most recent validation file
    """
    # Get the base outputs directory
    # From validate.py: OUTPUT_CSV = paths.rain_gauges_validation_dir / "ari_alarm_validation.csv"
    # paths.rain_gauges_validation_dir = outputs/rain_gauges/YYYYMMDD-YYYYMMDD/validation/
    # So we need: outputs/rain_gauges/*/validation/ari_alarm_validation.csv
    
    outputs_dir = Path("outputs")
    if not outputs_dir.exists():
        raise FileNotFoundError(
            f"Outputs directory not found: {outputs_dir}\n"
            "Please run validate.py first."
        )
    
    rain_gauges_dir = outputs_dir / "rain_gauges"
    if not rain_gauges_dir.exists():
        raise FileNotFoundError(
            f"Rain gauges output directory not found: {rain_gauges_dir}\n"
            "Please run validate.py first."
        )
    
    if not rain_gauges_dir.exists():
        raise FileNotFoundError(
            f"Rain gauges output directory not found: {rain_gauges_dir}\n"
            "Please run validate_ari_alarms_rain_gauges.py first."
        )
    
    # Search all date folders for validation files
    validation_files = list(rain_gauges_dir.glob("*/validation/ari_alarm_validation.csv"))
    
    if not validation_files:
        raise FileNotFoundError(
            f"No validation files found in {rain_gauges_dir}\n"
            "Please run validate_ari_alarms_rain_gauges.py first."
        )
    
    # Sort by modification time and get the most recent
    latest_file = max(validation_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"Auto-detected latest validation file: {latest_file}")
    
    return latest_file


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Visualize rain gauge ARI alarm validation results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Auto-detect latest validation file
  %(prog)s --date 2026-01-21                  # Use specific date folder
  %(prog)s --date 20260121                    # Date without dashes
  %(prog)s --input path/to/validation.csv     # Custom input file
  %(prog)s --output custom/output/dir/        # Custom output directory
  %(prog)s --log-level DEBUG                  # Verbose logging

Date Format:
  --date accepts: YYYY-MM-DD, YYYYMMDD, DD-MM-YYYY, etc.
  Searches for folder: YYYYMMDD-YYYYMMDD+1
  Example: --date 2026-01-21 looks for folder 20260121-20260122

Input File:
  Expects CSV from validate_ari_alarms_rain_gauges.py with columns:
  - assetid, gauge_name, alarm_time_utc, trace_id
  - status, max_ari_value, threshold, reason

Output Files:
  - validation_summary.png: Status pie chart
  - top_exceedances.png: Top 10 exceedances bar chart
  - validation_dashboard.html: Interactive HTML dashboard
  - validation_stats.csv: Full statistics

Duration:
  Typically <1 minute depending on number of alarms.
        """
    )
    
    parser.add_argument(
        "--date",
        type=str,
        metavar="DATE",
        help="Date to search for (YYYY-MM-DD or YYYYMMDD). Searches YYYYMMDD-YYYYMMDD+1 folder."
    )
    
    parser.add_argument(
        "--input",
        type=str,
        metavar="PATH",
        help="Path to validation CSV (overrides --date)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        metavar="DIR",
        help="Output directory for visualizations (default: same as input file directory)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.1.0"
    )
    
    return parser.parse_args()


def create_status_chart(df: pd.DataFrame, out_dir: Path, logger) -> None:
    """
    Create pie chart of validation status.
    
    Args:
        df: Validation results DataFrame
        out_dir: Output directory for chart
        logger: Logger instance
    """
    status_counts = df["status"].value_counts()
    
    colors = {
        "VALIDATED": "#28a745",
        "NOT_VALIDATED": "#dc3545", 
        "UNVALIDATABLE": "#6c757d",
    }
    
    plt.figure(figsize=(8, 6))
    plt.pie(
        status_counts.values,
        labels=status_counts.index,
        autopct="%1.1f%%",
        colors=[colors.get(s, "#999") for s in status_counts.index],
        startangle=90,
    )
    plt.title("ARI Alarm Validation Status", fontsize=14, fontweight="bold")
    plt.tight_layout()
    
    output_path = out_dir / "validation_summary.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    logger.info(f"✓ Saved {output_path.name}")


def create_exceedance_chart(df: pd.DataFrame, out_dir: Path, logger) -> None:
    """
    Create bar chart of top ARI exceedances.
    
    Args:
        df: Validation results DataFrame
        out_dir: Output directory for chart
        logger: Logger instance
    """
    df = df.copy()
    df["max_ari_value"] = pd.to_numeric(df["max_ari_value"], errors="coerce")
    df["threshold"] = pd.to_numeric(df["threshold"], errors="coerce")
    df["exceed_by"] = df["max_ari_value"] - df["threshold"]
    
    top = (
        df.dropna(subset=["exceed_by"])
        .sort_values("exceed_by", ascending=False)
        .head(10)
    )
    
    if top.empty:
        logger.warning("No exceedance data to plot")
        return
    
    plt.figure(figsize=(10, 6))
    bars = plt.barh(top["gauge_name"], top["exceed_by"], color="#667eea")
    plt.gca().invert_yaxis()
    plt.axvline(0, linestyle="--", linewidth=1, color="#999")
    
    plt.title("Top 10 ARI Exceedances (Above Threshold)", fontsize=14, fontweight="bold")
    plt.xlabel("Exceedance (ARI years above threshold)")
    plt.ylabel("Rain Gauge")
    
    plt.tight_layout()
    
    output_path = out_dir / "top_exceedances.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    logger.info(f"✓ Saved {output_path.name}")


def create_html_dashboard(df: pd.DataFrame, out_dir: Path, logger) -> Path:
    """
    Create interactive HTML dashboard for validation results.
    
    Args:
        df: Validation results DataFrame
        out_dir: Output directory for dashboard
        logger: Logger instance
        
    Returns:
        Path to generated dashboard HTML file
    """
    df = df.copy()
    df["max_ari_value"] = pd.to_numeric(df["max_ari_value"], errors="coerce")
    
    total = len(df)
    VALIDATED = (df["status"] == "VALIDATED").sum()
    not_VALIDATED = (df["status"] == "NOT_VALIDATED").sum()
    UNVALIDATABLE = (df["status"] == "UNVALIDATABLE").sum()
    
    avg_ari = df["max_ari_value"].mean() if df["max_ari_value"].notna().any() else 0
    max_ari = df["max_ari_value"].max() if df["max_ari_value"].notna().any() else 0
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>💧 Rain Gauge ARI Alarm Validation Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', sans-serif; background: #f5f5f5; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #28a745, #20c997); color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        .header h1 {{ font-size: 2em; margin-bottom: 5px; }}
        .header .meta {{ margin-top: 10px; opacity: 0.9; font-size: 0.9em; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); text-align: center; transition: transform 0.2s; }}
        .stat-card:hover {{ transform: translateY(-5px); box-shadow: 0 4px 10px rgba(0,0,0,0.15); }}
        .stat-card .value {{ font-size: 2em; font-weight: bold; }}
        .stat-card .label {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
        .stat-card.VALIDATED .value {{ color: #28a745; }}
        .stat-card.not-VALIDATED .value {{ color: #dc3545; }}
        .stat-card.UNVALIDATABLE .value {{ color: #6c757d; }}
        .section {{ background: white; padding: 25px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #333; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #28a745; }}
        .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }}
        .chart {{ text-align: center; }}
        .chart img {{ max-width: 100%; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ background: #667eea; color: white; padding: 12px; text-align: left; font-weight: 600; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f9f9f9; }}
        .status-VALIDATED {{ color: #28a745; font-weight: bold; }}
        .status-not-VALIDATED {{ color: #dc3545; font-weight: bold; }}
        .status-UNVALIDATABLE {{ color: #6c757d; }}
        .search-box {{ width: 100%; padding: 12px; border: 2px solid #ddd; border-radius: 8px; margin-bottom: 15px; font-size: 1em; }}
        .search-box:focus {{ outline: none; border-color: #28a745; }}
        .footer {{ text-align: center; color: #666; padding: 20px; font-size: 0.9em; border-top: 1px solid #ddd; margin-top: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌧️ Rain Gauge ARI Alarm Validation Dashboard</h1>
            <div class="meta">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="value">{total}</div>
                <div class="label">Total Alarms</div>
            </div>
            <div class="stat-card VALIDATED">
                <div class="value">{VALIDATED}</div>
                <div class="label">VALIDATED (✓)</div>
            </div>
            <div class="stat-card not-VALIDATED">
                <div class="value">{not_VALIDATED}</div>
                <div class="label">Not VALIDATED (✗)</div>
            </div>
            <div class="stat-card UNVALIDATABLE">
                <div class="value">{UNVALIDATABLE}</div>
                <div class="label">UNVALIDATABLE (—)</div>
            </div>
            <div class="stat-card">
                <div class="value">{avg_ari:.1f}</div>
                <div class="label">Avg Max ARI (years)</div>
            </div>
            <div class="stat-card">
                <div class="value">{max_ari:.1f}</div>
                <div class="label">Peak ARI (years)</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Visualizations</h2>
            <div class="charts">
                <div class="chart">
                    <h3>Validation Status Distribution</h3>
                    <img src="validation_summary.png" alt="Validation Summary">
                </div>
                <div class="chart">
                    <h3>Top 10 Exceedances</h3>
                    <img src="top_exceedances.png" alt="Top Exceedances">
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>📋 All Validation Results</h2>
            <input type="text" id="search" class="search-box" placeholder="🔍 Search gauge names, status, or alarm time...">
            <table id="resultsTable">
                <thead>
                    <tr>
                        <th>Gauge Name</th>
                        <th>Alarm Time (UTC)</th>
                        <th>Max ARI (years)</th>
                        <th>Threshold (years)</th>
                        <th>Status</th>
                        <th>Reason</th>
                    </tr>
                </thead>
                <tbody>
"""
    
    for _, row in df.sort_values("alarm_time_utc", ascending=False).iterrows():
        status_class = f"status-{row['status'].lower().replace('_', '-')}"
        max_ari_val = row['max_ari_value'] if pd.notna(row['max_ari_value']) else '-'
        if isinstance(max_ari_val, float):
            max_ari_val = f"{max_ari_val:.1f}"
        
        reason = row.get('reason', '') if pd.notna(row.get('reason', '')) else ''
        
        html += f"""                    <tr class="data-row">
                        <td>{row['gauge_name']}</td>
                        <td>{row['alarm_time_utc']}</td>
                        <td>{max_ari_val}</td>
                        <td>{row['threshold']}</td>
                        <td class="{status_class}">{row['status']}</td>
                        <td><small>{reason}</small></td>
                    </tr>
"""
    
    html += f"""                </tbody>
            </table>
        </div>
        
        <div class="footer">
            Rain Gauge ARI Alarm Validation | Auckland Council | {total} Alarms Analyzed | {datetime.now().strftime('%Y-%m-%d')}
        </div>
    </div>
    
    <script>
        // Search functionality
        document.getElementById('search').addEventListener('keyup', function() {{
            const query = this.value.toLowerCase();
            document.querySelectorAll('.data-row').forEach(row => {{
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(query) ? '' : 'none';
            }});
        }});
    </script>
</body>
</html>"""
    
    output_path = out_dir / "validation_dashboard.html"
    output_path.write_text(html, encoding="utf-8")
    logger.info(f"✓ Saved {output_path.name}")
    return output_path


def main() -> int:
    """
    Main entry point for rain gauge ARI alarm validation visualization.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()
    
    # Setup logging
    logger = setup_script_logger(args.log_level, __name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Rain Gauge ARI Alarm Validation Visualization")
        logger.info("=" * 80)
        
        # Determine input path
        if args.input:
            # Explicit input path provided
            input_path = Path(args.input)
            logger.info(f"Using explicit input path: {input_path}")
        elif args.date:
            # Date provided - search for specific date folder
            input_path = find_validation_file_by_date(args.date, logger)
        else:
            # No input or date - auto-detect latest
            logger.info("No date or input specified - searching for latest validation file...")
            input_path = find_latest_validation_file(logger)
        
        # Set output directory - default to same folder as input file
        if args.output:
            out_dir = Path(args.output)
        else:
            # Save visualizations in the same directory as the validation file
            out_dir = input_path.parent
            logger.info(f"Output will be saved to same directory as input: {out_dir}")
        
        # Validate input
        if not input_path.exists():
            raise FileNotFoundError(
                f"Input validation CSV not found: {input_path}\n"
                f"Please run validate_ari_alarms_rain_gauges.py first to generate this file."
            )
        
        # Create output directory
        out_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Input file: {input_path}")
        logger.info(f"Output directory: {out_dir}")
        logger.info("=" * 80)
        
        # Load validation data
        logger.info(f"Loading validation data from {input_path}...")
        df = pd.read_csv(input_path)
        
        # Validate required columns
        required_cols = ["status", "gauge_name", "alarm_time_utc", "max_ari_value", "threshold"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Missing required columns in validation CSV: {missing_cols}\n"
                f"Found columns: {df.columns.tolist()}"
            )
        
        logger.info(f"✓ Loaded {len(df)} validation records")
        logger.info("")
        
        # Quick stats
        status_counts = df["status"].value_counts()
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count}")
        
        logger.info("=" * 80)
        logger.info("Creating visualizations...")
        
        # Create visualizations
        create_status_chart(df, out_dir, logger)
        create_exceedance_chart(df, out_dir, logger)
        dashboard_path = create_html_dashboard(df, out_dir, logger)
        
        # Save stats copy
        stats_path = out_dir / "validation_stats.csv"
        df.to_csv(stats_path, index=False)
        logger.info(f"✓ Saved {stats_path.name}")
        
        # Success message
        logger.info("=" * 80)
        logger.info("✓ VISUALIZATION COMPLETE!")
        logger.info("=" * 80)
        logger.info(f"📊 Output directory: {out_dir}")
        logger.info(f"🌐 Dashboard: {dashboard_path}")
        logger.info("")
        logger.info("To view:")
        logger.info(f"  Open in browser: file://{dashboard_path.absolute()}")
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Visualization interrupted by user (Ctrl+C)")
        return 130
        
    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        return 1
        
    except ValueError as e:
        logger.error(f"❌ Invalid data: {e}")
        return 1
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ VISUALIZATION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting tips:")
        logger.error("1. Ensure validate_ari_alarms_rain_gauges.py completed successfully")
        logger.error("2. Check that validation CSV contains required columns")
        logger.error("3. Verify matplotlib is installed correctly")
        logger.error("4. Try running with --log-level DEBUG for more information")
        return 1


if __name__ == "__main__":
    sys.exit(main())