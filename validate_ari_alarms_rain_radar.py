#!/usr/bin/env python3
"""
Rain Radar ARI Alarm Validation Script

Validates rain radar ARI analysis results against alarm thresholds.
Checks which catchments would trigger alarms based on proportion of area
exceeding the ARI threshold.

Validation Logic:
    - Alarm triggers if ≥30% of catchment area has ARI ≥ 5 years (configurable)
    - Uses spatial proportion (areal coverage) not point measurement
    - Different threshold than rain gauges due to spatial nature of radar data

Usage:
    # Auto-detect most recent analysis
    python validate_ari_alarms_rain_radar.py
    
    # Validate specific historical date
    python validate_ari_alarms_rain_radar.py --date 2025-05-09
    
    # Custom input file
    python validate_ari_alarms_rain_radar.py --input outputs/rain_radar/analyze/ari_analysis_summary.csv
    
    # Custom proportion threshold (50% instead of default 30%)
    python validate_ari_alarms_rain_radar.py --threshold 0.50
    
    # Verbose logging
    python validate_ari_alarms_rain_radar.py --date 2025-05-09 --log-level DEBUG

Output:
    outputs/rain_radar/ari_alarm_validation.csv               (for current)
    outputs/rain_radar/historical/DATE/ari_alarm_validation.csv  (for historical)
    
    Columns:
    - catchment_id, catchment_name
    - max_ari (maximum ARI value in catchment)
    - pixels_total, pixels_exceeding, proportion_exceeding
    - alarm_status (ALARM or OK)
    - peak_duration, peak_depth_mm, peak_timestamp

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

from moata_pipeline.logging_setup import setup_logging


# Version info
__version__ = "1.0.0"

# Default settings
DEFAULT_PROPORTION_THRESHOLD = 0.30  # 30% of catchment area
DEFAULT_ARI_THRESHOLD = 5.0  # years (used in analysis)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Validate rain radar ARI analysis against alarm thresholds",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                  # Auto-detect most recent
  %(prog)s --date 2025-05-09                # Specific date
  %(prog)s --input custom/ari_summary.csv   # Custom input
  %(prog)s --threshold 0.50                 # 50%% area threshold
  %(prog)s --log-level DEBUG                # Verbose output

Validation Logic:
  - Alarm triggers if proportion_exceeding ≥ threshold
  - Default: ≥30%% of catchment area with ARI ≥5 years
  - Radar uses spatial proportion (different from point-based gauges)

Input Requirements:
  - ARI analysis summary CSV from analyze_rain_radar.py
  - Required columns: catchment_id, proportion_exceeding

Output:
  - Validation CSV with alarm status for each catchment
  - Summary report with counts and percentages
  - List of catchments that would trigger alarms

Duration:
  Typically <1 minute (simple CSV processing, no API calls)
        """
    )
    
    # Input options
    input_group = parser.add_argument_group('Input Options')
    
    input_group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Validate historical data for specific date. "
             "Example: --date 2025-05-09"
    )
    
    input_group.add_argument(
        "--input",
        metavar="PATH",
        help="Path to ari_analysis_summary.csv (overrides --date and auto-detect). "
             "Example: --input outputs/rain_radar/analyze/ari_analysis_summary.csv"
    )
    
    # Validation options
    validation_group = parser.add_argument_group('Validation Options')
    
    validation_group.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_PROPORTION_THRESHOLD,
        metavar="PROPORTION",
        help=f"Proportion threshold for alarm (default: {DEFAULT_PROPORTION_THRESHOLD} = 30%%). "
             f"Range: 0.0 to 1.0. Example: --threshold 0.50 for 50%%"
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    
    output_group.add_argument(
        "--output",
        metavar="PATH",
        help="Custom output path for validation CSV. "
             "Default: auto-determined based on input location"
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


def validate_threshold(threshold: float) -> None:
    """
    Validate proportion threshold value.
    
    Args:
        threshold: Proportion threshold (0.0 to 1.0)
        
    Raises:
        ValueError: If threshold is out of range
    """
    if not 0.0 <= threshold <= 1.0:
        raise ValueError(
            f"Proportion threshold must be between 0.0 and 1.0, got {threshold}\n"
            f"Example: --threshold 0.30 for 30%"
        )
    
    if threshold == 0.0:
        logging.warning(
            "⚠️  Threshold is 0.0 - ALL catchments will trigger alarms!"
        )
    elif threshold == 1.0:
        logging.warning(
            "⚠️  Threshold is 1.0 - NO catchments will trigger alarms!"
        )


def find_input_file(args: argparse.Namespace, logger: logging.Logger) -> Path:
    """
    Find ARI analysis summary file based on arguments.
    
    Args:
        args: Parsed arguments
        logger: Logger instance
        
    Returns:
        Path to input CSV file
        
    Raises:
        FileNotFoundError: If file cannot be found
    """
    # Option 1: Custom input path
    if args.input:
        input_path = Path(args.input)
        logger.info("Using custom input: %s", input_path)
        
    # Option 2: Specific date (historical)
    elif args.date:
        input_path = Path(
            f"outputs/rain_radar/historical/{args.date}/analyze/ari_analysis_summary.csv"
        )
        logger.info("Using historical data for date: %s", args.date)
        
    # Option 3: Auto-detect (prefer historical)
    else:
        logger.info("Auto-detecting ARI analysis summary...")
        
        # Check historical directories (most recent first)
        historical_files = sorted(
            Path("outputs/rain_radar/historical").glob("*/analyze/ari_analysis_summary.csv"),
            reverse=True
        )
        
        # Check current directory
        current_file = Path("outputs/rain_radar/analyze/ari_analysis_summary.csv")
        
        # Prefer most recent historical
        if historical_files:
            input_path = historical_files[0]
            date = input_path.parent.parent.name
            logger.info("✓ Found historical data: %s (date: %s)", input_path, date)
        elif current_file.exists():
            input_path = current_file
            logger.info("✓ Found current data: %s", input_path)
        else:
            raise FileNotFoundError(
                "No ARI analysis summary found.\n\n"
                "Have you run analysis first?\n"
                "  For current data:    python analyze_rain_radar.py\n"
                "  For specific date:   python analyze_rain_radar.py --date 2025-05-09"
            )
    
    # Validate file exists
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}\n\n"
            f"Have you run analysis first?\n"
            f"  python analyze_rain_radar.py" + 
            (f" --date {args.date}" if args.date else "")
        )
    
    return input_path


def run_validation(
    ari_summary_path: Path,
    output_path: Path,
    proportion_threshold: float,
    logger: logging.Logger,
) -> Dict:
    """
    Validate ARI analysis against proportion threshold.
    
    Args:
        ari_summary_path: Path to ARI summary CSV
        output_path: Path to save validation results
        proportion_threshold: Proportion threshold for alarm
        logger: Logger instance
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValueError: If required columns are missing
    """
    logger.info("Loading ARI summary from %s", ari_summary_path)
    df = pd.read_csv(ari_summary_path)
    logger.info("✓ Loaded %d catchment records", len(df))
    
    # Validate required columns
    required_cols = ["catchment_id", "proportion_exceeding"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing required columns in ARI summary: {missing_cols}\n"
            f"Found columns: {df.columns.tolist()}\n\n"
            f"The file may be incomplete. Try running analyze_rain_radar.py again."
        )
    
    # Apply validation logic
    df["would_alarm"] = df["proportion_exceeding"] >= proportion_threshold
    df["alarm_status"] = df["would_alarm"].map({True: "ALARM", False: "OK"})
    
    # Save results
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    cols = [
        "catchment_id", "catchment_name", "max_ari", "pixels_total",
        "pixels_exceeding", "proportion_exceeding", "alarm_status",
        "peak_duration", "peak_depth_mm", "peak_timestamp",
    ]
    available_cols = [c for c in cols if c in df.columns]
    df[available_cols].to_csv(output_path, index=False)
    
    logger.info("✓ Saved validation results to %s", output_path)
    
    # Generate report
    lines = []
    lines.append("=" * 70)
    lines.append("RAIN RADAR ALARM VALIDATION REPORT")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("=" * 70)
    lines.append("")
    lines.append("CONFIGURATION")
    lines.append("-" * 70)
    lines.append(f"ARI Threshold: {DEFAULT_ARI_THRESHOLD} years (used in analysis)")
    lines.append(f"Proportion Threshold: {proportion_threshold*100:.0f}% of catchment area")
    lines.append("")
    lines.append("SUMMARY")
    lines.append("-" * 70)
    lines.append(f"Total catchments: {len(df)}")
    
    alarm_count = int(df["would_alarm"].sum())
    ok_count = len(df) - alarm_count
    alarm_pct = (alarm_count / len(df) * 100) if len(df) > 0 else 0
    
    lines.append(f"Would trigger ALARM: {alarm_count} ({alarm_pct:.1f}%)")
    lines.append(f"Status OK: {ok_count}")
    lines.append("")
    
    # List alarms
    alarms = df[df["would_alarm"]].sort_values("proportion_exceeding", ascending=False)
    if not alarms.empty:
        lines.append("CATCHMENTS THAT WOULD ALARM")
        lines.append("-" * 70)
        for _, row in alarms.iterrows():
            name = row.get("catchment_name", "Unknown")
            prop = row.get("proportion_exceeding", 0)
            max_ari = row.get("max_ari", 0)
            lines.append(f"  {name}: {prop*100:.1f}% area, max ARI {max_ari:.1f}y")
    else:
        lines.append("NO ALARMS WOULD BE TRIGGERED")
        lines.append("-" * 70)
        lines.append("All catchments below proportion threshold.")
    
    lines.append("")
    lines.append("=" * 70)
    
    report = "\n".join(lines)
    
    return {
        "validation_df": df,
        "report": report,
        "output_path": output_path,
        "alarm_count": alarm_count,
        "ok_count": ok_count,
        "total_count": len(df),
    }


def main() -> int:
    """
    Main entry point for radar alarm validation.
    
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
        logger.info("Rain Radar Alarm Validation - v%s", __version__)
        logger.info("=" * 80)
        
        # Validate threshold
        validate_threshold(args.threshold)
        logger.info(f"Proportion threshold: {args.threshold*100:.0f}% of catchment area")
        
        # Find input file
        input_path = find_input_file(args, logger)
        
        # Determine output path
        if args.output:
            output_path = Path(args.output)
            logger.info("Custom output path: %s", output_path)
        else:
            # Put validation CSV next to analyze directory
            # e.g., outputs/rain_radar/historical/DATE/analyze/ 
            #    → outputs/rain_radar/historical/DATE/ari_alarm_validation.csv
            output_path = input_path.parent.parent / "ari_alarm_validation.csv"
            logger.info("Auto-determined output path: %s", output_path)
        
        logger.info("")
        logger.info("Configuration:")
        logger.info(f"  Input:     {input_path}")
        logger.info(f"  Output:    {output_path}")
        logger.info(f"  Threshold: {args.threshold*100:.0f}%")
        logger.info("=" * 80)
        logger.info("")
        
        # Run validation
        logger.info("Starting validation process...")
        
        result = run_validation(
            ari_summary_path=input_path,
            output_path=output_path,
            proportion_threshold=args.threshold,
            logger=logger,
        )
        
        # Display report
        logger.info("")
        logger.info(result["report"])
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Validation completed successfully")
        logger.info("=" * 80)
        logger.info(f"Results saved to: {result['output_path']}")
        logger.info("")
        logger.info("Summary:")
        logger.info(f"  Total catchments:   {result['total_count']}")
        logger.info(f"  Would alarm:        {result['alarm_count']}")
        logger.info(f"  Status OK:          {result['ok_count']}")
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("⚠️  Validation interrupted by user (Ctrl+C)")
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
        logger.error("❌ Validation Error")
        logger.error("=" * 80)
        logger.error(str(e))
        logger.error("")
        logger.error("Run with --help for usage information.")
        return 1
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Validation Failed")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("1. Verify analyze_rain_radar.py completed successfully")
        logger.error("2. Check ARI summary CSV has required columns")
        logger.error("3. Ensure proportion values are in range [0.0, 1.0]")
        logger.error("4. Try with --log-level DEBUG for more details")
        return 1


if __name__ == "__main__":
    sys.exit(main())