#!/usr/bin/env python3
"""
Rain Gauge ARI Alarm Validation Script

Validates historical ARI alarm events against actual trace data from Moata API.
Checks if alarms were triggered correctly by comparing alarm timestamps with
actual ARI values within a time window.

Usage:
    python validate_ari_alarms_rain_gauges.py [options]

Examples:
    # Validate with default settings
    python validate_ari_alarms_rain_gauges.py
    
    # Use custom input alarm CSV
    python validate_ari_alarms_rain_gauges.py --input data/custom_alarms.csv
    
    # Use custom trace mapping
    python validate_ari_alarms_rain_gauges.py --mapping outputs/rain_gauges/analyze/custom_mapping.csv
    
    # Adjust validation window (±2 hours instead of ±1 hour)
    python validate_ari_alarms_rain_gauges.py --window-before 2 --window-after 2
    
    # Change ARI threshold (default: 5.0 years)
    python validate_ari_alarms_rain_gauges.py --threshold 10.0
    
    # Enable debug logging
    python validate_ari_alarms_rain_gauges.py --log-level DEBUG

Validation Logic:
    For each alarm event:
    1. Find the corresponding ARI trace from mapping CSV
    2. Fetch ARI values ±N hours around alarm time
    3. Check if max ARI value ≥ threshold
    4. Status: VERIFIED (exceeded), NOT_VERIFIED (not exceeded), UNVERIFIABLE (error/no data)

Requirements:
    - Input alarm CSV with columns: assetid, name, createdtimeutc
    - Trace mapping CSV from analyze_rain_gauges.py with: gauge_id, trace_id, trace_description
    - Valid Moata API credentials in .env

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import argparse
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.common.constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    TOKEN_TTL_SECONDS,
    TOKEN_REFRESH_BUFFER_SECONDS,
    DEFAULT_REQUESTS_PER_SECOND,
)
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient

# Load environment variables
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

# Default paths and settings
DEFAULT_INPUT_CSV = Path("data/inputs/raingauge_ari_alarms.csv")
DEFAULT_TRACE_MAPPING_CSV = Path("outputs/rain_gauges/analyze/alarm_summary_full.csv")
DEFAULT_OUTPUT_CSV = Path("outputs/rain_gauges/ari_alarm_validation.csv")

# Default validation settings
DEFAULT_ARI_TRACE_DESC = "Max TP108 ARI"
DEFAULT_ARI_THRESHOLD = 5.0
DEFAULT_WINDOW_HOURS_BEFORE = 1
DEFAULT_WINDOW_HOURS_AFTER = 1
DEFAULT_DATA_INTERVAL_SECONDS = 300  # 5 minutes
DEFAULT_DATA_TYPE = "None"  # Raw data


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Validate historical ARI alarm events against actual trace data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                        # Default settings
  %(prog)s --input data/custom_alarms.csv         # Custom alarm input
  %(prog)s --threshold 10.0                       # 10-year ARI threshold
  %(prog)s --window-before 2 --window-after 2     # ±2 hour window
  %(prog)s --log-level DEBUG                      # Verbose output

Validation Status:
  - VERIFIED: Alarm correctly triggered (ARI ≥ threshold)
  - NOT_VERIFIED: Alarm incorrectly triggered (ARI < threshold)
  - UNVERIFIABLE: Cannot validate (missing data/trace/API error)

Input Files:
  - Alarm CSV: Historical alarm events (from Sam/Auckland Council)
  - Mapping CSV: Trace IDs from analyze_rain_gauges.py output

Output:
  - Validation results: CSV with status for each alarm
  - Summary statistics: Counts of VERIFIED/NOT_VERIFIED/UNVERIFIABLE

Duration:
  Depends on number of alarms and API response times.
  Typically 1-2 minutes for ~50 alarms.
        """
    )
    
    parser.add_argument(
        "--input",
        type=str,
        default=str(DEFAULT_INPUT_CSV),
        metavar="PATH",
        help=f"Path to alarm events CSV (default: {DEFAULT_INPUT_CSV})"
    )
    
    parser.add_argument(
        "--mapping",
        type=str,
        default=str(DEFAULT_TRACE_MAPPING_CSV),
        metavar="PATH",
        help=f"Path to trace mapping CSV (default: {DEFAULT_TRACE_MAPPING_CSV})"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_CSV),
        metavar="PATH",
        help=f"Path to output validation CSV (default: {DEFAULT_OUTPUT_CSV})"
    )
    
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_ARI_THRESHOLD,
        metavar="YEARS",
        help=f"ARI threshold in years (default: {DEFAULT_ARI_THRESHOLD})"
    )
    
    parser.add_argument(
        "--window-before",
        type=int,
        default=DEFAULT_WINDOW_HOURS_BEFORE,
        metavar="HOURS",
        help=f"Hours to look before alarm time (default: {DEFAULT_WINDOW_HOURS_BEFORE})"
    )
    
    parser.add_argument(
        "--window-after",
        type=int,
        default=DEFAULT_WINDOW_HOURS_AFTER,
        metavar="HOURS",
        help=f"Hours to look after alarm time (default: {DEFAULT_WINDOW_HOURS_AFTER})"
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


def iso_z(dt: pd.Timestamp) -> str:
    """
    Convert pandas Timestamp to ISO 8601 string with Z suffix.
    
    Args:
        dt: Pandas timestamp (assumed UTC)
        
    Returns:
        ISO formatted string (e.g., "2024-12-28T14:30:00Z")
    """
    if dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_trace_mapping(csv_path: Path, trace_desc: str, logger) -> Dict[int, int]:
    """
    Build mapping from asset_id to trace_id for ARI traces.
    
    Args:
        csv_path: Path to trace mapping CSV
        trace_desc: Description to filter traces (e.g., "Max TP108 ARI")
        logger: Logger instance
        
    Returns:
        Dictionary mapping asset_id -> trace_id
        
    Raises:
        FileNotFoundError: If CSV doesn't exist
        ValueError: If required columns missing
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Trace mapping CSV not found: {csv_path}\n"
            f"Please run analyze_rain_gauges.py first to generate this file."
        )
    
    logger.info(f"Loading trace mapping from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Validate required columns
    required_cols = ["gauge_id", "trace_id", "trace_description"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing required columns in trace mapping CSV: {missing_cols}\n"
            f"Found columns: {df.columns.tolist()}"
        )
    
    # Filter for ARI traces
    ari = df[df["trace_description"] == trace_desc].copy()
    ari = ari.dropna(subset=["gauge_id", "trace_id"])
    
    if len(ari) == 0:
        raise ValueError(
            f"No traces found with description '{trace_desc}' in {csv_path}\n"
            f"Available descriptions: {df['trace_description'].unique().tolist()}"
        )
    
    # Build mapping
    mapping = {}
    for _, row in ari.iterrows():
        asset_id = int(row["gauge_id"])
        trace_id = int(row["trace_id"])
        mapping[asset_id] = trace_id
    
    logger.info(f"✓ Found {len(mapping)} gauges with '{trace_desc}' traces")
    return mapping


def validate_alarms(
    client: MoataClient,
    alarms_df: pd.DataFrame,
    asset_to_trace: Dict[int, int],
    threshold: float,
    window_before: int,
    window_after: int,
    data_interval: int,
    data_type: str,
    logger
) -> List[Dict]:
    """
    Validate each alarm event against actual trace data.
    
    Args:
        client: Moata API client
        alarms_df: DataFrame with alarm events
        asset_to_trace: Mapping of asset_id -> trace_id
        threshold: ARI threshold for validation
        window_before: Hours before alarm to check
        window_after: Hours after alarm to check
        data_interval: Data interval in seconds
        data_type: Data type for API call
        logger: Logger instance
        
    Returns:
        List of validation result dictionaries
    """
    results: List[Dict] = []
    total = len(alarms_df)
    
    for idx, row in alarms_df.iterrows():
        asset_id = int(row["assetid"])
        gauge_name = str(row["name"])
        alarm_time = pd.to_datetime(row["createdtimeutc"], utc=True)
        
        logger.info("")
        logger.info(f"[{idx+1}/{total}] {gauge_name} (Asset ID: {asset_id})")
        logger.info(f"  Alarm time: {alarm_time}")
        
        # Get trace_id from mapping
        trace_id = asset_to_trace.get(asset_id)
        if not trace_id:
            logger.warning(f"  ⚠️  No trace mapping found for asset {asset_id}")
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time_utc": alarm_time,
                "trace_id": None,
                "status": "UNVERIFIABLE",
                "reason": "No trace mapping found",
                "max_ari_value": None,
                "threshold": threshold,
            })
            continue
        
        # Fetch data around alarm time
        from_time = iso_z(alarm_time - timedelta(hours=window_before))
        to_time = iso_z(alarm_time + timedelta(hours=window_after))
        
        try:
            data = client.get_trace_data(
                trace_id=trace_id,
                from_time=from_time,
                to_time=to_time,
                data_type=data_type,
                data_interval=data_interval,
            )
        except Exception as e:
            logger.warning(f"  ⚠️  Failed to fetch data: {e}")
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time_utc": alarm_time,
                "trace_id": trace_id,
                "status": "UNVERIFIABLE",
                "reason": f"API error: {str(e)[:100]}",
                "max_ari_value": None,
                "threshold": threshold,
            })
            continue
        
        items = data.get("items", [])
        if not items:
            logger.warning(f"  ⚠️  No data returned for time window")
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time_utc": alarm_time,
                "trace_id": trace_id,
                "status": "UNVERIFIABLE",
                "reason": "No data in time window",
                "max_ari_value": None,
                "threshold": threshold,
            })
            continue
        
        # Find max value (same logic as original - handle potential None)
        values = [item.get("value", 0) for item in items]
        max_value = max(values) if values else 0
        
        # Check if threshold was exceeded
        exceeded = max_value >= threshold
        status = "VERIFIED" if exceeded else "NOT_VERIFIED"
        
        logger.info(f"  Trace ID: {trace_id}")
        logger.info(f"  Max ARI value: {max_value:.2f} years")
        logger.info(f"  Threshold: {threshold} years")
        logger.info(f"  Status: {'✅ ' if exceeded else '❌ '}{status}")
        
        results.append({
            "assetid": asset_id,
            "gauge_name": gauge_name,
            "alarm_time_utc": alarm_time,
            "trace_id": trace_id,
            "status": status,
            "reason": "",
            "max_ari_value": round(max_value, 2),
            "threshold": threshold,
        })
    
    return results


def main() -> int:
    """
    Main entry point for ARI alarm validation.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Starting Rain Gauge ARI Alarm Validation")
        logger.info("=" * 80)
        logger.info(f"Input alarm CSV: {args.input}")
        logger.info(f"Trace mapping CSV: {args.mapping}")
        logger.info(f"Output CSV: {args.output}")
        logger.info(f"ARI threshold: {args.threshold} years")
        logger.info(f"Time window: -{args.window_before}h to +{args.window_after}h")
        logger.info("=" * 80)
        
        # Validate credentials
        client_id = os.getenv("MOATA_CLIENT_ID")
        client_secret = os.getenv("MOATA_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError(
                "MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set in .env\n"
                "See .env.example for configuration details."
            )
        
        # Initialize Moata client (verify_ssl=False for Auckland Council network)
        logger.info("Initializing Moata API client...")
        auth = MoataAuth(
            token_url=TOKEN_URL,
            scope=OAUTH_SCOPE,
            client_id=client_id,
            client_secret=client_secret,
            verify_ssl=False,  # Disabled for Auckland Council network compatibility
            ttl_seconds=TOKEN_TTL_SECONDS,
            refresh_buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS,
        )
        
        http = MoataHttp(
            get_token_fn=auth.get_token,
            base_url=BASE_API_URL,
            requests_per_second=DEFAULT_REQUESTS_PER_SECOND,
            verify_ssl=False,  # Disabled for Auckland Council network compatibility
        )
        
        client = MoataClient(http=http)
        logger.info("✓ Moata API client ready")
        
        # Build trace mapping
        mapping_path = Path(args.mapping)
        asset_to_trace = build_trace_mapping(
            csv_path=mapping_path,
            trace_desc=DEFAULT_ARI_TRACE_DESC,
            logger=logger
        )
        
        # Load alarm events
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(
                f"Input alarm CSV not found: {input_path}\n"
                f"Please ensure the file exists and path is correct."
            )
        
        logger.info(f"Loading alarm events from {input_path}...")
        alarms_df = pd.read_csv(input_path)
        
        # Validate required columns
        required_cols = ["assetid", "name", "createdtimeutc"]
        missing_cols = [col for col in required_cols if col not in alarms_df.columns]
        if missing_cols:
            raise ValueError(
                f"Missing required columns in alarm CSV: {missing_cols}\n"
                f"Found columns: {alarms_df.columns.tolist()}"
            )
        
        logger.info(f"✓ Loaded {len(alarms_df)} alarm events")
        logger.info("=" * 80)
        
        # Validate alarms
        logger.info("Starting validation process...")
        results = validate_alarms(
            client=client,
            alarms_df=alarms_df,
            asset_to_trace=asset_to_trace,
            threshold=args.threshold,
            window_before=args.window_before,
            window_after=args.window_after,
            data_interval=DEFAULT_DATA_INTERVAL_SECONDS,
            data_type=DEFAULT_DATA_TYPE,
            logger=logger
        )
        
        # Save results
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        out_df = pd.DataFrame(results)
        out_df.to_csv(output_path, index=False)
        
        # Display summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 80)
        status_counts = out_df["status"].value_counts()
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count}")
        
        # Calculate percentages
        total = len(out_df)
        if total > 0:
            logger.info("")
            logger.info("Percentages:")
            for status, count in status_counts.items():
                pct = (count / total) * 100
                logger.info(f"  {status}: {pct:.1f}%")
        
        logger.info("=" * 80)
        logger.info(f"✅ Results saved to: {output_path}")
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Validation interrupted by user (Ctrl+C)")
        logger.info("Partial results may have been saved")
        return 130
        
    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        return 1
        
    except ValueError as e:
        logger.error(f"❌ Invalid data: {e}")
        return 1
        
    except RuntimeError as e:
        logger.error(f"❌ Configuration error: {e}")
        return 1
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ VALIDATION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting tips:")
        logger.error("1. Ensure analyze_rain_gauges.py completed successfully")
        logger.error("2. Check that alarm CSV contains required columns")
        logger.error("3. Verify Moata API credentials in .env")
        logger.error("4. Check network connection to Moata API")
        logger.error("5. Try running with --log-level DEBUG for more information")
        return 1


if __name__ == "__main__":
    sys.exit(main())