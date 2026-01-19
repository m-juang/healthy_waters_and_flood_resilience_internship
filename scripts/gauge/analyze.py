#!/usr/bin/env python3
"""
Rain Gauge Analysis and Filtering Script

Analyzes collected rain gauge data, applies quality filters, and identifies active gauges.
Filters out inactive gauges and those matching exclusion criteria (e.g., test gauges).

Usage:
    python analyze_rain_gauges.py [options]

Examples:
    # Analyze with default settings
    python analyze_rain_gauges.py

    # Analyze (alias) current mode - same behavior, kept for CLI consistency
    python analyze_rain_gauges.py --current

    # Override inactivity threshold (default: 3 months)
    python analyze_rain_gauges.py --inactive-months 6

    # Change exclusion keyword (default: "test")
    python analyze_rain_gauges.py --exclude-keyword "backup"

    # Enable debug logging
    python analyze_rain_gauges.py --log-level DEBUG

Filters Applied:
    - Temporal coverage: ≥80% non-null values
    - Recency: Data within specified months (default: 3)
    - Value range: 0 ≤ rainfall ≤ 500 mm/hour
    - Name filtering: Excludes gauges matching keyword (default: "test")

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-02
"""

import sys
from pathlib import Path
# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
import sys
from typing import Dict, Any

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.analyze.runner import run_filter_active_gauges
from moata_pipeline.common.constants import (
    INACTIVE_THRESHOLD_MONTHS,
    DEFAULT_EXCLUDE_KEYWORD
)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Analyze and filter rain gauge data for active, quality gauges",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Auto-detect (prefers historical)
  %(prog)s --current                          # Analyze current (last 24h) data
  %(prog)s --date 2025-05-09                  # Analyze specific historical date
  %(prog)s --inactive-months 6                # Consider 6-month inactivity
  %(prog)s --exclude-keyword "backup"         # Exclude gauges with "backup"
  %(prog)s --log-level DEBUG                  # Verbose output

Filters Applied:
  - Temporal coverage: ≥80%% non-null values
  - Recency: Data within last N months (default: 3)
  - Value range: 0-500 mm/hour (outlier removal)
  - Name filter: Exclude gauges matching keyword (default: "test")

Input:
  Current mode: outputs/rain_gauges/raw/
  Historical mode: outputs/rain_gauges/historical/DATE/raw/

Output:
  Current mode: outputs/rain_gauges/analyze/
  Historical mode: outputs/rain_gauges/historical/DATE/analyze/

Duration:
  Typically 2-3 minutes depending on dataset size.
        """
    )
    
    # Data source options (mutually exclusive)
    source_group = parser.add_argument_group('Data Source (choose one or auto-detect)')
    source_mutex = source_group.add_mutually_exclusive_group()
    
    source_mutex.add_argument(
        "--current",
        action="store_true",
        help="Analyze current (last 24h) data explicitly. "
             "Uses outputs/rain_gauges/raw/"
    )
    
    source_mutex.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Analyze specific historical date. "
             "Example: --date 2025-05-09"
    )
    
    # Filter options
    filter_group = parser.add_argument_group('Filter Options')
    
    filter_group.add_argument(
        "--inactive-months",
        type=int,
        default=INACTIVE_THRESHOLD_MONTHS,
        metavar="N",
        help=f"Consider gauge inactive if no data in last N months (default: {INACTIVE_THRESHOLD_MONTHS})"
    )
    
    filter_group.add_argument(
        "--exclude-keyword",
        type=str,
        default=DEFAULT_EXCLUDE_KEYWORD,
        metavar="KEYWORD",
        help=f'Exclude gauges with KEYWORD in name (default: "{DEFAULT_EXCLUDE_KEYWORD}")'
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
        version="%(prog)s 1.1.0"
    )
    
    return parser.parse_args()


def format_output_paths(result: Dict[str, Any]) -> str:
    """
    Format output file paths for logging.
    
    Args:
        result: Result dictionary from run_filter_active_gauges
        
    Returns:
        Formatted string with output paths
    """
    output_lines = []
    
    # Check for output_dir first (most common)
    if "output_dir" in result:
        output_lines.append(f"  📁 Output directory: {result['output_dir']}")
    
    # Look for specific file paths
    path_keys = [k for k in result.keys() if "path" in k or "file" in k or "csv" in k]
    if path_keys:
        output_lines.append("  📄 Output files:")
        for key in sorted(path_keys):
            # Format key: "output_csv_path" -> "Output CSV"
            formatted_key = key.replace("_", " ").title().replace("Path", "").strip()
            output_lines.append(f"     - {formatted_key}: {result[key]}")
    
    return "\n".join(output_lines) if output_lines else "  (No output paths in result)"


def main() -> int:
    """
    Main entry point for rain gauge analysis.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()
    
    # Setup logging with user-specified level
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Starting Rain Gauge Data Filtering and Analysis")
        logger.info("=" * 80)
        
        # Determine mode
        if args.date:
            logger.info(f"Mode: Historical ({args.date})")
            input_date = args.date
        elif args.current:
            logger.info("Mode: Current (last 24h)")
            input_date = None
        else:
            logger.info("Mode: Auto-detect (prefers historical)")
            input_date = None
        
        logger.info(f"Inactive threshold: {args.inactive_months} months")
        logger.info(f"Exclude keyword: '{args.exclude_keyword}'")
        logger.info("=" * 80)
        
        # Run analysis with date parameter
        result = run_filter_active_gauges(
            inactive_months=args.inactive_months,
            exclude_keyword=args.exclude_keyword,
            input_date=input_date
        )
        
        # Display analysis report
        if "report" in result:
            logger.info("\n%s", result["report"])
        
        # Display success message
        logger.info("=" * 80)
        logger.info("✅ ANALYSIS COMPLETE!")
        logger.info("=" * 80)
        
        # Display output paths
        logger.info("\n" + format_output_paths(result))
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Analysis interrupted by user (Ctrl+C)")
        logger.info("Partial results may have been saved")
        return 130  # Standard exit code for SIGINT
        
    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        logger.error("\nPossible causes:")
        logger.error("1. Raw gauge data not collected yet - run retrieve_rain_gauges.py first")
        logger.error("2. Missing outputs/rain_gauges/raw/ or historical directory")
        logger.error("3. No JSON files in raw data directory")
        return 1
        
    except PermissionError as e:
        logger.error(f"❌ Permission denied: {e}")
        logger.error("Check file/directory permissions for outputs/rain_gauges/")
        return 1
        
    except ValueError as e:
        logger.error(f"❌ Invalid data: {e}")
        logger.error("\nPossible causes:")
        logger.error("1. Corrupted raw data files")
        logger.error("2. Invalid filter parameters (check --inactive-months)")
        logger.error("3. Empty or malformed JSON files")
        logger.error("4. Invalid date format (use YYYY-MM-DD)")
        return 1
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ ANALYSIS FAILED")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting tips:")
        logger.error("1. Ensure you've run retrieve_rain_gauges.py first")
        logger.error("2. Check that outputs/rain_gauges/raw/ contains valid JSON files")
        logger.error("3. Verify filter parameters are reasonable (e.g., --inactive-months > 0)")
        logger.error("4. Try running with --log-level DEBUG for more information")
        logger.error("5. Check disk space in outputs/ directory")
        return 1


if __name__ == "__main__":
    sys.exit(main())

