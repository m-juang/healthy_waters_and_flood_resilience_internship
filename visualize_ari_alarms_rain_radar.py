#!/usr/bin/env python3
"""
Rain Radar ARI Alarm Validation Visualization Script

Creates interactive HTML dashboard and charts showing radar ARI exceedances
and catchment validation statistics.

Features:
    - Interactive HTML dashboard with search
    - ARI distribution histogram
    - Top catchments bar chart
    - Proportion exceeding distribution
    - Summary statistics

Usage:
    # Auto-detect most recent validation
    python visualize_ari_alarms_rain_radar.py
    
    # Visualize specific date
    python visualize_ari_alarms_rain_radar.py --date 2025-05-09
    
    # Custom input file
    python visualize_ari_alarms_rain_radar.py --input outputs/rain_radar/ari_alarm_validation.csv
    
    # Custom output directory
    python visualize_ari_alarms_rain_radar.py --date 2025-05-09 --output custom/viz/
    
    # Verbose logging
    python visualize_ari_alarms_rain_radar.py --log-level DEBUG

Output:
    outputs/rain_radar/validation_viz/                           (for current)
    outputs/rain_radar/historical/DATE/validation_viz/           (for historical)
    ├── validation_dashboard.html    # Interactive dashboard with search
    ├── ari_distribution.png         # ARI value histogram
    ├── top_catchments.png           # Top 15 catchments bar chart
    ├── proportion_distribution.png  # Area exceeding histogram
    └── validation_stats.csv         # Complete statistics

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt

from moata_pipeline.logging_setup import setup_logging


# Version info
__version__ = "1.0.0"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Visualize rain radar ARI alarm validation results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                  # Auto-detect most recent
  %(prog)s --date 2025-05-09                # Specific date
  %(prog)s --input custom/validation.csv    # Custom input
  %(prog)s --output custom/viz/             # Custom output
  %(prog)s --log-level DEBUG                # Verbose output

Output Files:
  - validation_dashboard.html: Interactive HTML dashboard with search
  - ari_distribution.png: Histogram of max ARI values
  - top_catchments.png: Top 15 catchments by max ARI
  - proportion_distribution.png: Distribution of area exceeding
  - validation_stats.csv: Complete statistics

Duration:
  Typically <1 minute (creates charts and HTML)
        """
    )
    
    # Input options
    input_group = parser.add_argument_group('Input Options')
    
    input_group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Visualize validation for specific historical date. "
             "Example: --date 2025-05-09"
    )
    
    input_group.add_argument(
        "--input",
        metavar="PATH",
        help="Path to ari_alarm_validation.csv (overrides --date and auto-detect). "
             "Example: --input outputs/rain_radar/ari_alarm_validation.csv"
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    
    output_group.add_argument(
        "--output",
        metavar="PATH",
        help="Custom output directory path. "
             "Default: auto-determined based on input location. "
             "Example: --output custom/validation_viz/"
    )
    
    # Logging options
    log_group = parser.add_argument_group('Logging Options')
    
    log_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)"
    )
    
    # Metadata
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    return parser.parse_args()


def find_input_file(args: argparse.Namespace, logger: logging.Logger) -> tuple[Path, Optional[str]]:
    """
    Find validation CSV file and extract date if historical.
    
    Args:
        args: Parsed arguments
        logger: Logger instance
        
    Returns:
        Tuple of (input_path, data_date)
        
    Raises:
        FileNotFoundError: If validation file not found
    """
    data_date: Optional[str] = None
    
    # Option 1: Custom input path
    if args.input:
        input_path = Path(args.input)
        logger.info("Using custom input: %s", input_path)
        # Try to extract date from path
        if "historical" in str(input_path):
            parts = input_path.parts
            if "historical" in parts:
                idx = parts.index("historical")
                if idx + 1 < len(parts):
                    data_date = parts[idx + 1]
        
    # Option 2: Specific date (historical)
    elif args.date:
        input_path = Path(f"outputs/rain_radar/historical/{args.date}/ari_alarm_validation.csv")
        data_date = args.date
        logger.info("Using historical validation for date: %s", args.date)
        
    # Option 3: Auto-detect (prefer historical)
    else:
        logger.info("Auto-detecting validation file...")
        
        # Check historical (most recent first)
        historical_files = sorted(
            Path("outputs/rain_radar/historical").glob("*/ari_alarm_validation.csv"),
            reverse=True
        )
        
        # Check current
        current_file = Path("outputs/rain_radar/ari_alarm_validation.csv")
        
        # Prefer most recent historical
        if historical_files:
            input_path = historical_files[0]
            data_date = input_path.parent.name
            logger.info("✓ Found historical validation: %s (date: %s)", input_path, data_date)
        elif current_file.exists():
            input_path = current_file
            logger.info("✓ Found current validation: %s", input_path)
        else:
            raise FileNotFoundError(
                "No validation file found.\n\n"
                "Have you run validation first?\n"
                "  python validate_ari_alarms_rain_radar.py"
            )
    
    # Validate file exists
    if not input_path.exists():
        raise FileNotFoundError(
            f"Validation file not found: {input_path}\n\n"
            f"Have you run validation first?\n"
            f"  python validate_ari_alarms_rain_radar.py" +
            (f" --date {args.date}" if args.date else "")
        )
    
    return input_path, data_date


def create_ari_distribution_chart(df: pd.DataFrame, out_dir: Path, logger: logging.Logger) -> None:
    """
    Create histogram of max ARI values.
    
    Args:
        df: Validation DataFrame
        out_dir: Output directory
        logger: Logger instance
    """
    ari_values = df[df["max_ari"] > 0]["max_ari"]
    
    if ari_values.empty:
        logger.warning("⚠️  No ARI values > 0 to plot")
        return
    
    plt.figure(figsize=(10, 6))
    plt.hist(ari_values, bins=30, color="#667eea", edgecolor="white", alpha=0.8)
    plt.axvline(5, color="#dc3545", linestyle="--", linewidth=2, label="Threshold (5 years)")
    
    plt.title("Distribution of Max ARI Values Across Catchments", fontsize=14, fontweight="bold")
    plt.xlabel("Max ARI (years)")
    plt.ylabel("Number of Catchments")
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(out_dir / "ari_distribution.png", dpi=200, bbox_inches='tight')
    plt.close()
    logger.info("✓ Created ari_distribution.png")


def create_top_catchments_chart(df: pd.DataFrame, out_dir: Path, logger: logging.Logger) -> None:
    """
    Create bar chart of top catchments by max ARI.
    
    Args:
        df: Validation DataFrame
        out_dir: Output directory
        logger: Logger instance
    """
    top = df.nlargest(15, "max_ari")
    
    if top.empty:
        logger.warning("⚠️  No data for top catchments chart")
        return
    
    plt.figure(figsize=(12, 8))
    colors = ["#dc3545" if row["proportion_exceeding"] >= 0.30 else "#667eea" for _, row in top.iterrows()]
    
    bars = plt.barh(range(len(top)), top["max_ari"], color=colors)
    plt.yticks(range(len(top)), top["catchment_name"])
    plt.gca().invert_yaxis()
    plt.axvline(5, color="#dc3545", linestyle="--", linewidth=2, label="ARI Threshold (5 years)")
    
    plt.title("Top 15 Catchments by Max ARI", fontsize=14, fontweight="bold")
    plt.xlabel("Max ARI (years)")
    plt.ylabel("Catchment")
    plt.legend()
    plt.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(out_dir / "top_catchments.png", dpi=200, bbox_inches='tight')
    plt.close()
    logger.info("✓ Created top_catchments.png")


def create_proportion_chart(df: pd.DataFrame, out_dir: Path, logger: logging.Logger) -> None:
    """
    Create histogram of proportion exceeding distribution.
    
    Args:
        df: Validation DataFrame
        out_dir: Output directory
        logger: Logger instance
    """
    proportions = df["proportion_exceeding"] * 100
    
    plt.figure(figsize=(10, 6))
    plt.hist(proportions, bins=20, color="#28a745", edgecolor="white", alpha=0.8)
    plt.axvline(30, color="#dc3545", linestyle="--", linewidth=2, label="Alarm Threshold (30%)")
    
    plt.title("Distribution of Area Exceeding ARI Threshold", fontsize=14, fontweight="bold")
    plt.xlabel("Proportion of Catchment Area Exceeding (%)")
    plt.ylabel("Number of Catchments")
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(out_dir / "proportion_distribution.png", dpi=200, bbox_inches='tight')
    plt.close()
    logger.info("✓ Created proportion_distribution.png")


def create_html_dashboard(
    df: pd.DataFrame,
    out_dir: Path,
    data_date: Optional[str],
    logger: logging.Logger
) -> Path:
    """
    Create interactive HTML dashboard for radar validation results.
    
    Args:
        df: Validation DataFrame
        out_dir: Output directory
        data_date: Date string if historical
        logger: Logger instance
        
    Returns:
        Path to created dashboard
    """
    total = len(df)
    would_alarm = int((df["alarm_status"] == "ALARM").sum())
    ok = total - would_alarm
    
    avg_ari = float(df["max_ari"].mean())
    max_ari = float(df["max_ari"].max())
    avg_proportion = float(df["proportion_exceeding"].mean() * 100)
    
    date_display = f"Data Date: {data_date}" if data_date else "Data: Last 24 hours"
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rain Radar ARI Validation Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f5f5; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #1e3c72, #2a5298); color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        .header h1 {{ font-size: 2em; margin-bottom: 5px; }}
        .header .meta {{ margin-top: 10px; opacity: 0.9; font-size: 0.95em; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); text-align: center; transition: transform 0.2s; }}
        .stat-card:hover {{ transform: translateY(-2px); box-shadow: 0 4px 10px rgba(0,0,0,0.15); }}
        .stat-card .value {{ font-size: 2em; font-weight: bold; color: #1e3c72; }}
        .stat-card .label {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
        .stat-card.alarm .value {{ color: #dc3545; }}
        .stat-card.ok .value {{ color: #28a745; }}
        .section {{ background: white; padding: 25px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #333; margin-bottom: 15px; font-size: 1.5em; }}
        .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }}
        .chart img {{ max-width: 100%; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ background: #667eea; color: white; padding: 12px; text-align: left; font-weight: 600; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f9f9f9; }}
        .status-alarm {{ color: #dc3545; font-weight: bold; }}
        .status-ok {{ color: #28a745; font-weight: 600; }}
        .search-box {{ width: 100%; padding: 12px; border: 2px solid #ddd; border-radius: 8px; margin-bottom: 15px; font-size: 1em; }}
        .search-box:focus {{ outline: none; border-color: #667eea; }}
        .footer {{ text-align: center; color: #666; padding: 20px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌧️ Rain Radar ARI Validation Dashboard</h1>
            <div class="meta">{date_display} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="value">{total}</div>
                <div class="label">Total Catchments</div>
            </div>
            <div class="stat-card alarm">
                <div class="value">{would_alarm}</div>
                <div class="label">Would Alarm</div>
            </div>
            <div class="stat-card ok">
                <div class="value">{ok}</div>
                <div class="label">OK</div>
            </div>
            <div class="stat-card">
                <div class="value">{max_ari:.1f}</div>
                <div class="label">Peak ARI (years)</div>
            </div>
            <div class="stat-card">
                <div class="value">{avg_ari:.1f}</div>
                <div class="label">Avg Max ARI</div>
            </div>
            <div class="stat-card">
                <div class="value">{avg_proportion:.1f}%</div>
                <div class="label">Avg Area Exceeding</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Charts</h2>
            <div class="charts">
                <div class="chart"><img src="ari_distribution.png" alt="ARI Distribution"></div>
                <div class="chart"><img src="proportion_distribution.png" alt="Proportion Distribution"></div>
                <div class="chart"><img src="top_catchments.png" alt="Top Catchments"></div>
            </div>
        </div>
        
        <div class="section">
            <h2>🚨 Catchments That Would Alarm (≥30% area exceeding)</h2>
            <table>
                <thead>
                    <tr><th>Catchment</th><th>Max ARI</th><th>Pixels Total</th><th>Pixels Exceeding</th><th>Proportion</th><th>Peak Duration</th></tr>
                </thead>
                <tbody>
"""
    
    alarms = df[df["alarm_status"] == "ALARM"].sort_values("proportion_exceeding", ascending=False)
    for _, row in alarms.iterrows():
        html += f"""                    <tr>
                        <td>{row['catchment_name']}</td>
                        <td>{row['max_ari']:.1f}</td>
                        <td>{int(row['pixels_total'])}</td>
                        <td>{int(row['pixels_exceeding'])}</td>
                        <td class="status-alarm">{row['proportion_exceeding']*100:.1f}%</td>
                        <td>{row.get('peak_duration', '-')}</td>
                    </tr>
"""
    
    if alarms.empty:
        html += """                    <tr><td colspan="6" style="text-align: center; color: #666; padding: 20px;">No catchments exceed alarm threshold</td></tr>
"""
    
    html += f"""                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>📋 All Catchments</h2>
            <input type="text" id="search" class="search-box" placeholder="🔍 Search catchments...">
            <table id="allTable">
                <thead>
                    <tr><th>Catchment</th><th>Max ARI</th><th>Pixels</th><th>Exceeding</th><th>Proportion</th><th>Status</th></tr>
                </thead>
                <tbody>
"""
    
    for _, row in df.sort_values("max_ari", ascending=False).iterrows():
        status_class = "status-alarm" if row["alarm_status"] == "ALARM" else "status-ok"
        html += f"""                    <tr class="data-row">
                        <td>{row['catchment_name']}</td>
                        <td>{row['max_ari']:.1f}</td>
                        <td>{int(row['pixels_total'])}</td>
                        <td>{int(row['pixels_exceeding'])}</td>
                        <td>{row['proportion_exceeding']*100:.1f}%</td>
                        <td class="{status_class}">{row['alarm_status']}</td>
                    </tr>
"""
    
    html += f"""                </tbody>
            </table>
        </div>
        
        <div class="footer">
            Rain Radar ARI Validation Dashboard v{__version__} | Auckland Council | {total} Catchments | {datetime.now().strftime('%Y-%m-%d')}
        </div>
    </div>
    
    <script>
        document.getElementById('search').addEventListener('keyup', function() {{
            const q = this.value.toLowerCase();
            document.querySelectorAll('.data-row').forEach(row => {{
                row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
            }});
        }});
    </script>
</body>
</html>"""
    
    output_path = out_dir / "validation_dashboard.html"
    output_path.write_text(html, encoding="utf-8")
    logger.info("✓ Created validation_dashboard.html")
    return output_path


def main() -> int:
    """
    Main entry point for radar validation visualization.
    
    Returns:
        Exit code (0=success, 1=error, 130=interrupted)
    """
    # Parse arguments
    try:
        args = parse_args()
    except SystemExit as e:
        return e.code if e.code is not None else 0
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Rain Radar ARI Validation Visualization - v%s", __version__)
        logger.info("=" * 80)
        
        # Find input file
        input_path, data_date = find_input_file(args, logger)
        
        # Determine output directory
        if args.output:
            out_dir = Path(args.output)
            logger.info("Using custom output directory: %s", out_dir)
        else:
            out_dir = input_path.parent / "validation_viz"
            logger.info("Auto-determined output directory: %s", out_dir)
        
        # Create output directory
        out_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("")
        logger.info("Configuration:")
        logger.info(f"  Input:  {input_path}")
        logger.info(f"  Output: {out_dir}")
        if data_date:
            logger.info(f"  Date:   {data_date}")
        logger.info("=" * 80)
        logger.info("")
        
        # Load validation data
        logger.info("Loading validation data...")
        df = pd.read_csv(input_path)
        logger.info("✓ Loaded %d catchment records", len(df))
        
        # Validate required columns
        required_cols = ["catchment_name", "max_ari", "proportion_exceeding", "alarm_status"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"Missing required columns: {missing}\n"
                f"Found: {df.columns.tolist()}"
            )
        
        # Create visualizations
        logger.info("")
        logger.info("Creating visualizations...")
        create_ari_distribution_chart(df, out_dir, logger)
        create_top_catchments_chart(df, out_dir, logger)
        create_proportion_chart(df, out_dir, logger)
        dashboard_path = create_html_dashboard(df, out_dir, data_date, logger)
        
        # Save stats
        stats_path = out_dir / "validation_stats.csv"
        df.to_csv(stats_path, index=False)
        logger.info("✓ Saved validation_stats.csv")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Visualization completed successfully")
        logger.info("=" * 80)
        logger.info(f"Output directory: {out_dir}")
        logger.info("")
        logger.info("Generated files:")
        logger.info("  - validation_dashboard.html    (interactive dashboard)")
        logger.info("  - ari_distribution.png         (ARI histogram)")
        logger.info("  - top_catchments.png           (top 15 bar chart)")
        logger.info("  - proportion_distribution.png  (proportion histogram)")
        logger.info("  - validation_stats.csv         (complete data)")
        logger.info("=" * 80)
        
        # Print to stdout
        print(f"\n✅ Done! Open in browser: {dashboard_path.absolute()}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("⚠️  Visualization interrupted by user (Ctrl+C)")
        logger.warning("=" * 80)
        return 130
        
    except FileNotFoundError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ File Not Found")
        logger.error("=" * 80)
        logger.error(str(e))
        return 1
        
    except ValueError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Data Error")
        logger.error("=" * 80)
        logger.error(str(e))
        return 1
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Visualization Failed")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("1. Verify validation completed successfully")
        logger.error("2. Check validation CSV has required columns")
        logger.error("3. Ensure matplotlib is installed")
        logger.error("4. Check disk space for output files")
        logger.error("5. Try with --log-level DEBUG for more details")
        return 1


if __name__ == "__main__":
    sys.exit(main())