#!/usr/bin/env python3
"""
Rain Radar Data Analysis Script

Analyzes radar QPE (Quantitative Precipitation Estimation) data for Auckland 
stormwater catchments and calculates ARI (Annual Recurrence Interval) values.

Features:
    - Automatic data directory detection
    - ARI calculation using TP108 methodology
    - Catchment-level aggregation
    - ARI exceedance identification
    - Summary statistics and reporting

Usage:
    # Auto-detect most recent data (prefers historical)
    python analyze_rain_radar.py
    
    # Analyze specific historical date
    python analyze_rain_radar.py --date 2025-05-09
    
    # Analyze current (last 24h) data
    python analyze_rain_radar.py --current
    
    # Analyze custom directory
    python analyze_rain_radar.py --data-dir outputs/rain_radar/raw/radar_data
    
    # Custom ARI threshold and output
    python analyze_rain_radar.py --date 2025-05-09 --threshold 10.0 --output-dir custom/

Output:
    outputs/rain_radar/analyze/                    (for current data)
    outputs/rain_radar/historical/DATE/analyze/    (for historical data)
    ├── ari_analysis_summary.csv      # Per-catchment ARI summary
    ├── ari_exceedances.csv           # Catchments exceeding threshold
    └── analysis_report.txt           # Detailed text report

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.analyze.radar_analysis import run_radar_analysis


# Version info
__version__ = "1.0.0"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Analyze rain radar (QPE) data and calculate ARI for catchments",
        epilog="""
Examples:
  # Auto-detect most recent data (prefers historical)
  %(prog)s
  
  # Analyze specific historical date
  %(prog)s --date 2025-05-09
  
  # Analyze current (last 24h) data explicitly
  %(prog)s --current
  
  # Analyze custom radar data directory
  %(prog)s --data-dir outputs/rain_radar/raw/radar_data
  
  # Custom ARI threshold (10-year instead of default 5-year)
  %(prog)s --date 2025-05-09 --threshold 10.0
  
  # Custom output directory
  %(prog)s --current --output-dir custom/analysis/
  
  # Verbose logging for debugging
  %(prog)s --date 2025-05-09 --log-level DEBUG

Notes:
  - Auto-detection prefers historical data over current
  - Analysis requires prior data collection (run retrieve_rain_radar.py first)
  - ARI threshold determines which catchments are flagged
  - Duration: ~10-15 minutes for full day of data
  - Requires: ~2 GB RAM for processing
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Data source options (mutually exclusive)
    source_group = parser.add_argument_group('Data Source (choose one or auto-detect)')
    source_mutex = source_group.add_mutually_exclusive_group()
    
    source_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Analyze historical data for specific date. "
             "Example: --date 2025-05-09"
    )
    
    source_mutex.add_argument(
        "--current",
        action="store_true",
        help="Analyze current (last 24h) data explicitly. "
             "Without this or --date, auto-detects most recent."
    )
    
    source_mutex.add_argument(
        "--data-dir",
        metavar="PATH",
        help="Path to custom radar data directory. "
             "Overrides --date and --current. "
             "Example: --data-dir outputs/rain_radar/raw/radar_data"
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    
    output_group.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Custom output directory path. "
             "Default: auto-determined based on input location. "
             "Example: --output-dir custom/analysis/"
    )
    
    # Analysis options
    analysis_group = parser.add_argument_group('Analysis Options')
    
    analysis_group.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        metavar="YEARS",
        help="ARI threshold in years for flagging catchments (default: 5.0). "
             "Catchments exceeding this threshold are highlighted. "
             "Example: --threshold 10.0"
    )
    
    # Logging options
    log_group = parser.add_argument_group('Logging Options')
    
    log_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO). "
             "Use DEBUG for verbose output."
    )
    
    # Metadata
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    return parser.parse_args()


def validate_threshold(threshold: float) -> None:
    """
    Validate ARI threshold value.
    
    Args:
        threshold: ARI threshold in years
        
    Raises:
        ValueError: If threshold is invalid
    """
    if threshold <= 0:
        raise ValueError(
            f"ARI threshold must be positive, got {threshold}"
        )
    
    if threshold > 100:
        logging.warning(
            f"⚠️  Very high ARI threshold: {threshold} years. "
            f"This may result in very few or no exceedances."
        )


def detect_radar_data_dir(args: argparse.Namespace, logger: logging.Logger) -> Path:
    """
    Detect radar data directory based on arguments.
    
    Args:
        args: Parsed command-line arguments
        logger: Logger instance
        
    Returns:
        Path to radar data directory
        
    Raises:
        FileNotFoundError: If data directory doesn't exist
    """
    # Option 1: Custom directory
    if args.data_dir:
        radar_data_dir = Path(args.data_dir)
        logger.info("Using custom data directory: %s", radar_data_dir)
        
    # Option 2: Specific date (historical)
    elif args.date:
        radar_data_dir = Path(f"outputs/rain_radar/historical/{args.date}/raw/radar_data")
        logger.info("Using historical data for date: %s", args.date)
        
    # Option 3: Current data (explicit)
    elif args.current:
        radar_data_dir = Path("outputs/rain_radar/raw/radar_data")
        logger.info("Using current (last 24h) data")
        
    # Option 4: Auto-detect (prefer historical)
    else:
        logger.info("Auto-detecting radar data directory...")
        
        # Check for historical data
        historical_base = Path("outputs/rain_radar/historical")
        if historical_base.exists():
            historical_dirs = sorted(historical_base.glob("*/raw/radar_data"))
            if historical_dirs:
                radar_data_dir = historical_dirs[-1]  # Most recent
                date_dir = radar_data_dir.parent.parent.name
                logger.info("✓ Found historical data: %s (date: %s)", radar_data_dir, date_dir)
            else:
                # Fallback to current
                radar_data_dir = Path("outputs/rain_radar/raw/radar_data")
                logger.info("No historical data found, using current: %s", radar_data_dir)
        else:
            # Fallback to current
            radar_data_dir = Path("outputs/rain_radar/raw/radar_data")
            logger.info("Using current data: %s", radar_data_dir)
    
    # Validate directory exists
    if not radar_data_dir.exists():
        raise FileNotFoundError(
            f"Radar data directory not found: {radar_data_dir}\n\n"
            f"Have you run data collection first?\n"
            f"  For current data:    python retrieve_rain_radar.py\n"
            f"  For specific date:   python retrieve_rain_radar.py --date {args.date or '2025-05-09'}"
        )
    
    # Check if directory has data
    data_files = list(radar_data_dir.glob("*.json"))
    if not data_files:
        raise FileNotFoundError(
            f"No radar data files found in: {radar_data_dir}\n"
            f"Directory exists but is empty."
        )
    
    logger.info(f"✓ Found {len(data_files)} radar data files")
    
    return radar_data_dir


def determine_output_dir(
    radar_data_dir: Path,
    args: argparse.Namespace,
    logger: logging.Logger
) -> Path:
    """
    Determine output directory based on input location.
    
    Args:
        radar_data_dir: Input radar data directory
        args: Parsed arguments
        logger: Logger instance
        
    Returns:
        Path to output directory
    """
    if args.output_dir:
        output_dir = Path(args.output_dir)
        logger.info("Using custom output directory: %s", output_dir)
    else:
        # Auto-determine: put analyze/ next to raw/
        # outputs/rain_radar/raw/radar_data → outputs/rain_radar/analyze/
        # outputs/rain_radar/historical/DATE/raw/radar_data → outputs/rain_radar/historical/DATE/analyze/
        output_dir = radar_data_dir.parent.parent / "analyze"
        logger.info("Auto-determined output directory: %s", output_dir)
    
    return output_dir


def main() -> int:
    """
    Main entry point for radar data analysis.
    
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
        logger.info("Rain Radar ARI Analysis - v%s", __version__)
        logger.info("=" * 80)
        
        # Validate threshold
        validate_threshold(args.threshold)
        logger.info(f"ARI threshold: {args.threshold} years")
        
        # Detect radar data directory
        radar_data_dir = detect_radar_data_dir(args, logger)
        
        # Determine output directory
        output_dir = determine_output_dir(radar_data_dir, args, logger)
        
        logger.info("")
        logger.info("Configuration:")
        logger.info(f"  Input:     {radar_data_dir}")
        logger.info(f"  Output:    {output_dir}")
        logger.info(f"  Threshold: {args.threshold} years")
        logger.info("=" * 80)
        logger.info("")
        
        # Run analysis
        logger.info("Starting ARI analysis...")
        
        result = run_radar_analysis(
            radar_data_dir=radar_data_dir,
            output_dir=output_dir,
            ari_threshold=args.threshold,
        )
        
        # Display report
        logger.info("")
        logger.info("=" * 80)
        logger.info("ANALYSIS RESULTS")
        logger.info("=" * 80)
        logger.info("")
        logger.info(result["report"])
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Analysis completed successfully")
        logger.info("=" * 80)
        logger.info(f"Output files saved to: {result['output_dir']}")
        logger.info("")
        logger.info("Generated files:")
        logger.info("  - ari_analysis_summary.csv  (per-catchment ARI summary)")
        logger.info("  - ari_exceedances.csv       (catchments exceeding threshold)")
        logger.info("  - analysis_report.txt       (detailed text report)")
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("⚠️  Analysis interrupted by user (Ctrl+C)")
        logger.warning("=" * 80)
        return 130
        
    except FileNotFoundError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Data Not Found")
        logger.error("=" * 80)
        logger.error(str(e))
        return 1
        
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
        logger.error("❌ Analysis Failed")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("1. Verify radar data was collected successfully")
        logger.error("2. Check data directory exists and has .json files")
        logger.error("3. Ensure TP108 coefficients file is available")
        logger.error("4. Check disk space for output files")
        logger.error("5. Try with --log-level DEBUG for more details")
        return 1


if __name__ == "__main__":
    sys.exit(main())