#!/usr/bin/env python3
"""
Rain Gauge Visualization Script

Generates interactive HTML dashboards from analyzed rain gauge data.
Creates main dashboard with summary charts and individual pages for each gauge.

Usage:
    python visualize_rain_gauges.py [options]

Examples:
    # Auto-detect latest analysis data
    python visualize_rain_gauges.py
    
    # Specify custom input CSV
    python visualize_rain_gauges.py --csv outputs/rain_gauges/analyze/rain_gauge_analysis_20241228.csv
    
    # Specify custom output directory
    python visualize_rain_gauges.py --out outputs/rain_gauges/custom_viz/
    
    # Enable debug logging
    python visualize_rain_gauges.py --log-level DEBUG

Output:
    - Main dashboard: outputs/rain_gauges/visualizations/dashboard.html
    - Per-gauge pages: outputs/rain_gauges/visualizations/gauges/GAUGE_XXX.html
    - Embedded charts: matplotlib charts embedded in HTML

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import argparse
import logging
import sys
from pathlib import Path

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.viz.runner import run_visual_report


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
  %(prog)s                                      # Auto-detect latest data
  %(prog)s --csv path/to/analysis.csv           # Custom input CSV
  %(prog)s --out custom/output/dir/             # Custom output directory
  %(prog)s --log-level DEBUG                    # Verbose logging

Auto-Detection:
  If --csv is not specified, the script searches for the most recent
  rain_gauge_analysis_*.csv file in outputs/rain_gauges/analyze/

Input Requirements:
  - CSV file must contain columns: gauge_id, gauge_name, latitude, longitude
  - Data should be from analyze_rain_gauges.py output

Output:
  - Main dashboard: dashboard.html (summary charts, gauge list)
  - Per-gauge pages: gauges/GAUGE_XXX.html (detailed timeseries)
  - All charts embedded as base64 images in HTML

Duration:
  Typically 3-5 minutes depending on number of gauges (~200 gauges).
        """
    )
    
    parser.add_argument(
        "--csv",
        type=str,
        default="",
        metavar="PATH",
        help="Path to analysis CSV file (auto-detects if not specified)"
    )
    
    parser.add_argument(
        "--out",
        type=str,
        default="",
        metavar="DIR",
        help="Output directory for visualizations (auto-detects if not specified)"
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
        version="%(prog)s 1.0.0"
    )
    
    return parser.parse_args()


def validate_csv_path(csv_path: Path | None) -> None:
    """
    Validate that CSV file exists and is readable.
    
    Args:
        csv_path: Path to CSV file (None if auto-detecting)
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If path is not a CSV file
    """
    if csv_path is None:
        return  # Auto-detection will handle this
    
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}\n"
            f"Please run analyze_rain_gauges.py first to generate analysis data."
        )
    
    if csv_path.suffix.lower() != '.csv':
        raise ValueError(
            f"Invalid file type: {csv_path.suffix}\n"
            f"Expected .csv file, got: {csv_path}"
        )
    
    if csv_path.stat().st_size == 0:
        raise ValueError(
            f"CSV file is empty: {csv_path}\n"
            f"Please check that analyze_rain_gauges.py completed successfully."
        )


def main() -> int:
    """
    Main entry point for rain gauge visualization.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()
    
    # Setup logging with user-specified level
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Starting Rain Gauge Visualization")
        logger.info("=" * 80)
        
        # Parse paths
        csv_path = Path(args.csv) if args.csv else None
        out_dir = Path(args.out) if args.out else None
        
        # Log configuration
        if csv_path:
            logger.info(f"Input CSV: {csv_path}")
        else:
            logger.info("Input CSV: Auto-detecting latest analysis file")
        
        if out_dir:
            logger.info(f"Output directory: {out_dir}")
        else:
            logger.info("Output directory: Auto-detecting (outputs/rain_gauges/visualizations/)")
        
        logger.info("=" * 80)
        
        # Validate CSV path if provided
        if csv_path:
            validate_csv_path(csv_path)
            logger.info(f"✓ CSV file validated: {csv_path.name} ({csv_path.stat().st_size:,} bytes)")
        
        # Run visualization
        logger.info("Generating visualizations...")
        report_path = run_visual_report(csv_path=csv_path, out_dir=out_dir)
        
        # Success message
        logger.info("=" * 80)
        logger.info("✅ VISUALIZATION COMPLETE!")
        logger.info("=" * 80)
        logger.info(f"📊 Dashboard created: {report_path}")
        logger.info("")
        logger.info("To view:")
        logger.info(f"  1. Open in browser: file://{report_path.absolute()}")
        logger.info(f"  2. Or double-click: {report_path}")
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Visualization interrupted by user (Ctrl+C)")
        logger.info("Partial visualizations may have been created")
        return 130  # Standard exit code for SIGINT
        
    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        logger.error("\nPossible causes:")
        logger.error("1. Analysis data not generated yet - run analyze_rain_gauges.py first")
        logger.error("2. Incorrect --csv path specified")
        logger.error("3. Missing outputs/rain_gauges/analyze/ directory")
        logger.error("\nTo generate analysis data:")
        logger.error("  python analyze_rain_gauges.py")
        return 1
        
    except ValueError as e:
        logger.error(f"❌ Invalid input: {e}")
        logger.error("\nCheck that:")
        logger.error("1. CSV file is valid and not corrupted")
        logger.error("2. CSV contains required columns (gauge_id, gauge_name, etc.)")
        logger.error("3. File is from analyze_rain_gauges.py output")
        return 1
        
    except PermissionError as e:
        logger.error(f"❌ Permission denied: {e}")
        logger.error("Check file/directory permissions for outputs/rain_gauges/visualizations/")
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
        logger.error("1. Ensure analyze_rain_gauges.py completed successfully")
        logger.error("2. Check that CSV file contains valid gauge data")
        logger.error("3. Verify matplotlib and pandas are installed correctly")
        logger.error("4. Try running with --log-level DEBUG for more information")
        logger.error("5. Check available disk space in outputs/ directory")
        return 1


if __name__ == "__main__":
    sys.exit(main())