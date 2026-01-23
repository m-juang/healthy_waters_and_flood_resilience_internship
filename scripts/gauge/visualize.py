#!/usr/bin/env python3
"""
Rain Gauge Visualization Script

Generates interactive HTML dashboards from analyzed rain gauge data.
Creates main dashboard with summary charts and individual pages for each gauge.

Usage:
    # Auto-detect most recent data (prefers historical)
    python visualize_rain_gauges.py
    
    # Visualize current (last 24h) data
    python visualize_rain_gauges.py --current
    
    # Visualize specific historical date
    python visualize_rain_gauges.py --date 2025-05-09
    
    # Specify custom input CSV
    python visualize_rain_gauges.py --csv path/to/analysis.csv
    
    # Specify custom output directory
    python visualize_rain_gauges.py --out custom/viz/
    
    # Enable debug logging
    python visualize_rain_gauges.py --log-level DEBUG

Output:
    outputs/rain_gauges/visualizations/                    (for current)
    outputs/rain_gauges/historical/DATE/visualizations/    (for historical)
    - Main dashboard: dashboard.html
    - Per-gauge pages: gauges/GAUGE_XXX.html

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-14
Version: 1.1.0 - Added --current and --date flags for date-specific visualization
"""

import sys
from pathlib import Path
# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
import sys
from pathlib import Path

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.viz.runner import run_visual_report


# Version info
__version__ = "1.1.0"
paths = PipelinePaths()


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Generate interactive HTML dashboards from rain gauge analysis data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Auto-detect most recent data
  %(prog)s --current                # Visualize current (last 24h) data
  %(prog)s --date 2025-05-09        # Visualize specific historical date
  %(prog)s --csv path/to/file.csv   # Custom input CSV
  %(prog)s --out custom/dir/        # Custom output directory
  %(prog)s --log-level DEBUG        # Verbose logging

Auto-Detection (no flags):
  Searches for most recent alarm_summary.csv in:
  outputs/rain_gauges/YYYYMMDD-YYYYMMDD/analysis/

Current Mode (--current):
  Uses: outputs/rain_gauges/YYYYMMDD-YYYYMMDD/analysis/alarm_summary.csv
  Output: outputs/rain_gauges/YYYYMMDD-YYYYMMDD/visualizations/

Historical Mode (--date YYYY-MM-DD):
  Uses: outputs/rain_gauges/YYYYMMDD-YYYYMMDD/analysis/alarm_summary.csv
  Output: outputs/rain_gauges/YYYYMMDD-YYYYMMDD/visualizations/

Input Requirements:
  - CSV file must contain: gauge_id, gauge_name, latitude, longitude
  - Data should be from analyze_rain_gauges.py output

Duration:
  Typically 3-5 minutes for ~200 gauges
        """
    )
    
    # Data source options (mutually exclusive)
    source_group = parser.add_argument_group('Data Source (choose one or auto-detect)')
    source_mutex = source_group.add_mutually_exclusive_group()
    
    source_mutex.add_argument(
        "--current",
        action="store_true",
        help="Visualize current (last 24h) data explicitly. "
             "Uses outputs/rain_gauges/analyze/"
    )
    
    source_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Visualize specific historical date. "
             "Example: --date 2025-05-09"
    )
    
    source_mutex.add_argument(
        "--csv",
        type=str,
        metavar="PATH",
        help="Path to specific analysis CSV file (overrides other options). "
             "Example: --csv path/to/analysis.csv"
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    
    output_group.add_argument(
        "--out",
        type=str,
        metavar="DIR",
        help="Custom output directory (auto-determined if not specified). "
             "Example: --out custom/output/"
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


def detect_analysis_csv(args: argparse.Namespace, logger: logging.Logger) -> Path:
    """
    Detect analysis CSV file based on arguments.
    
    Args:
        args: Parsed command-line arguments
        logger: Logger instance
        
    Returns:
        Path to analysis CSV file (alarm_summary.csv)
        
    Raises:
        FileNotFoundError: If CSV file not found
    """
    # Option 1: Custom CSV path
    if args.csv:
        csv_path = Path(args.csv)
        logger.info("Using custom CSV: %s", csv_path)
        
    # Option 2: Current data (explicit)
    elif args.current:
        analyze_dir = Path("outputs/rain_gauges/analyze")
        logger.info("Using current (last 24h) data")
        
        if not analyze_dir.exists():
            raise FileNotFoundError(
                f"Analysis directory not found: {analyze_dir}\n\n"
                f"Have you run analysis first?\n"
                f"  python scripts/gauge/analyze.py"
            )
        
        # Look for alarm_summary.csv (the file that visualizer actually uses)
        csv_path = analyze_dir / "alarm_summary.csv"
        
        if not csv_path.exists():
            raise FileNotFoundError(
                f"No alarm_summary.csv found in: {analyze_dir}\n\n"
                f"Have you run analysis first?\n"
                f"  python scripts/gauge/analyze.py"
            )
        
        logger.info("✓ Found alarm summary: %s", csv_path.name)
        
    # Option 3: Specific date (historical)
    elif args.date:
        # Use PipelinePaths for proper folder structure
        from moata_pipeline.common.paths import PipelinePaths
        date_paths = PipelinePaths.for_date(args.date)
        analyze_dir = date_paths.rain_gauges_analysis_dir
        logger.info("Using historical data for date: %s", args.date)
        
        # Try specified date first
        if not analyze_dir.exists():
            # Try previous date (analysis stored with window start date)
            from datetime import datetime, timedelta
            try:
                specified_date = datetime.strptime(args.date, "%Y-%m-%d")
                prev_date = (specified_date - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.warning(
                    f"No data found for {args.date}, trying previous date {prev_date}..."
                )
                date_paths = PipelinePaths.for_date(prev_date)
                analyze_dir = date_paths.rain_gauges_analysis_dir
            except Exception as e:
                pass
        
        if not analyze_dir.exists():
            raise FileNotFoundError(
                f"Analysis directory not found: {analyze_dir}\n\n"
                f"Have you run analysis first?\n"
                f"  python scripts/gauge/analyze.py --date {args.date}"
            )
        
        # Look for alarm_summary.csv
        csv_path = analyze_dir / "alarm_summary.csv"
        
        if not csv_path.exists():
            raise FileNotFoundError(
                f"No alarm_summary.csv found in: {analyze_dir}\n\n"
                f"Have you run analysis first?\n"
                f"  python scripts/gauge/analyze.py --date {args.date}"
            )
        
        logger.info("✓ Found alarm summary: %s", csv_path.name)
        
    # Option 4: Auto-detect (prefer most recent date range folder)
    else:
        logger.info("Auto-detecting alarm summary CSV...")
        
        # Check for date range folders: outputs/rain_gauges/YYYYMMDD-YYYYMMDD/analysis/
        from moata_pipeline.common.paths import PipelinePaths
        historical_base = Path("outputs/rain_gauges")
        historical_files = []
        
        if historical_base.exists():
            # Look for YYYYMMDD-YYYYMMDD pattern folders with alarm_summary.csv
            historical_files = sorted(
                [f for f in historical_base.glob("*-*/analysis/alarm_summary.csv") if f.exists()],
                key=lambda p: p.parent.parent.name,  # Sort by folder name
                reverse=True
            )
        
        # Prefer most recent
        if historical_files:
            csv_path = historical_files[0]
            date_range = csv_path.parent.parent.name
            logger.info("✓ Found alarm summary: %s (range: %s)", csv_path.name, date_range)
        else:
            raise FileNotFoundError(
                "No alarm_summary.csv found.\n\n"
                "Have you run analysis first?\n"
                "  For current:       python scripts/gauge/analyze.py\n"
                "  For specific date: python scripts/gauge/analyze.py --date 2025-05-09\n"
                "  For auto-detect:   python scripts/gauge/analyze.py"
            )
    
    # Validate file exists
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}\n\n"
            f"Please run analyze_rain_gauges.py first."
        )
    
    # Validate file is not empty
    if csv_path.stat().st_size == 0:
        raise ValueError(
            f"CSV file is empty: {csv_path}\n\n"
            f"Please check that analyze_rain_gauges.py completed successfully."
        )
    
    return csv_path


def determine_output_dir(csv_path: Path, args: argparse.Namespace, logger: logging.Logger) -> Path:
    """
    Determine output directory based on input location.
    
    Args:
        csv_path: Path to input CSV
        args: Parsed arguments
        logger: Logger instance
        
    Returns:
        Path to output directory
    """
    if args.out:
        out_dir = Path(args.out)
        logger.info("Using custom output directory: %s", out_dir)
    else:
        # Auto-determine: put visualizations/ next to analyze/
        # outputs/rain_gauges/analyze/ → outputs/rain_gauges/visualizations/
        # outputs/rain_gauges/historical/DATE/analyze/ → outputs/rain_gauges/historical/DATE/visualizations/
        out_dir = csv_path.parent.parent / "visualizations"
        logger.info("Auto-determined output directory: %s", out_dir)
    
    return out_dir


def main() -> int:
    """
    Main entry point for rain gauge visualization.
    
    Returns:
        Exit code (0 for success, 1 for failure, 130 for interrupt)
    """
    args = parse_args()
    
    # Setup logging
    logger = setup_script_logger(args.log_level, __name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Rain Gauge Visualization - v%s", __version__)
        logger.info("=" * 80)
        
        # Detect analysis CSV
        csv_path = detect_analysis_csv(args, logger)
        
        # Determine output directory
        out_dir = determine_output_dir(csv_path, args, logger)
        
        # Extract date for historical data
        input_date = None
        if args.date:
            input_date = args.date
        elif not args.current and "historical" in str(csv_path):
            # Auto-detected historical - extract date from path
            parts = csv_path.parts
            if "historical" in parts:
                idx = parts.index("historical")
                if idx + 1 < len(parts):
                    input_date = parts[idx + 1]
        
        logger.info("")
        logger.info("Configuration:")
        logger.info(f"  Input:  {csv_path}")
        logger.info(f"  Output: {out_dir}")
        logger.info(f"  Size:   {csv_path.stat().st_size:,} bytes")
        if input_date:
            logger.info(f"  Date:   {input_date}")
        logger.info("=" * 80)
        logger.info("")
        
        # Run visualization with correct parameters
        logger.info("Generating visualizations...")
        report_path = run_visual_report(
            csv_path=csv_path,
            out_dir=out_dir,
            input_date=input_date
        )
        
        # Success message
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Visualization Complete!")
        logger.info("=" * 80)
        logger.info(f"Dashboard: {report_path}")
        logger.info("")
        logger.info("To view:")
        logger.info(f"  Open in browser: {report_path.absolute()}")
        logger.info("  Or double-click the file in File Explorer")
        logger.info("=" * 80)
        
        # Print to stdout
        print(f"\n✓ Done! Open in browser: {report_path.absolute()}")
        
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
        logger.error("❌ Invalid Input")
        logger.error("=" * 80)
        logger.error(str(e))
        return 1
        
    except PermissionError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Permission Denied")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.error("Check file/directory permissions")
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
        logger.error("1. Verify analyze_rain_gauges.py completed successfully")
        logger.error("2. Check CSV file is valid and not corrupted")
        logger.error("3. Ensure matplotlib and pandas are installed")
        logger.error("4. Try with --log-level DEBUG for more details")
        return 1


if __name__ == "__main__":
    sys.exit(main())