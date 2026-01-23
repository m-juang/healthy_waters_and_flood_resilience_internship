#!/usr/bin/env python3
"""
ARI Alarm Validation Script

Validates ARI (Annual Recurrence Interval) alarms by:
1. Loading the alarm log from Sam's CSV file
2. Fetching 'Max TP108 ARI' trace data around each alarm time
3. Checking if the trace value exceeded the threshold at that time

This implements Sam's suggestion to:
- "Pull the data from the 'Max TP108 ARI' trace"
- "Automate checking the times when it has exceeded its threshold"
- "Match what you see on the Moata website"

Usage:
    python scripts/alarms/validate_ari_alarms.py
    python scripts/alarms/validate_ari_alarms.py --alarm-file data/inputs/raingauge_ari_alarms.csv
    python scripts/alarms/validate_ari_alarms.py --asset-id 3160974  # Validate single gauge

Author: Auckland Council Internship Team (COMPSCI 778)
Date: 2026-01-21
Version: 1.0.0
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from moata_pipeline.common.script_utils import (
    setup_script_logger,
    print_script_header,
    print_script_footer,
)
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient
from moata_pipeline.common.config import Config


# =============================================================================
# Constants
# =============================================================================

__version__ = "1.0.0"

# API Configuration
TOKEN_URL = "https://login.moata.io/connect/token"
OAUTH_SCOPE = "mapi offline_access"
BASE_API_URL = "https://api.moata.io/ae/v1"

# Default alarm file path
DEFAULT_ALARM_FILE = Path("data/inputs/raingauge_ari_alarms.csv")

# Time window around alarm to check (hours before/after)
CHECK_WINDOW_HOURS = 2


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class AlarmRecord:
    """Represents an ARI alarm from Sam's CSV file."""
    asset_id: int
    name: str
    description: str
    alert_id: int
    created_time_utc: datetime
    
    @classmethod
    def from_csv_row(cls, row: Dict[str, str]) -> "AlarmRecord":
        """Create AlarmRecord from CSV row."""
        return cls(
            asset_id=int(row["assetid"]),
            name=row["name"],
            description=row["description"],
            alert_id=int(row["alertid"]),
            created_time_utc=datetime.strptime(
                row["createdtimeutc"], "%Y-%m-%d %H:%M:%S.%f"
            ).replace(tzinfo=timezone.utc)
        )


@dataclass
class ValidationResult:
    """Result of validating an alarm."""
    alarm: AlarmRecord
    trace_id: Optional[int] = None
    threshold: Optional[float] = None
    max_value: Optional[float] = None
    exceeded: Optional[bool] = None
    error: Optional[str] = None
    
    @property
    def status(self) -> str:
        """Get validation status string."""
        if self.error:
            return f"ERROR: {self.error}"
        if self.exceeded is None:
            return "UNKNOWN"
        return "✓ VALID" if self.exceeded else "✗ MISMATCH"


# =============================================================================
# Alarm Loader
# =============================================================================

def load_alarm_records(file_path: Path) -> List[AlarmRecord]:
    """
    Load alarm records from Sam's CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of AlarmRecord objects
    """
    records = []
    
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                record = AlarmRecord.from_csv_row(row)
                records.append(record)
            except (KeyError, ValueError) as e:
                logging.warning(f"Skipping invalid row: {e}")
    
    return records


# =============================================================================
# Client Creation
# =============================================================================

def create_client() -> MoataClient:
    """Create authenticated Moata API client."""
    load_dotenv(dotenv_path=Path.cwd() / ".env")
    
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise ValueError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set")
    
    auth = MoataAuth(
        token_url=TOKEN_URL,
        scope=OAUTH_SCOPE,
        client_id=client_id,
        client_secret=client_secret,
        verify_ssl=False,
        ttl_seconds=3600,
        refresh_buffer_seconds=300,
    )
    
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        requests_per_second=5,
        verify_ssl=False,
    )
    
    return MoataClient(http=http)


# =============================================================================
# Trace Finder
# =============================================================================

def find_max_tp108_ari_trace(
    client: MoataClient,
    asset_id: int,
    logger: logging.Logger
) -> Optional[Dict[str, Any]]:
    """
    Find the 'Max TP108 ARI' trace for an asset.
    
    Args:
        client: Moata API client
        asset_id: Asset ID to search
        logger: Logger instance
        
    Returns:
        Trace dict if found, None otherwise
    """
    try:
        traces = client.traces.get_traces_for_asset(asset_id)
        
        for trace in traces:
            description = trace.get("description", "").lower()
            if "max tp108 ari" in description or "tp108" in description:
                logger.debug(f"Found TP108 trace: {trace.get('id')} - {trace.get('description')}")
                return trace
        
        logger.warning(f"No Max TP108 ARI trace found for asset {asset_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error finding trace for asset {asset_id}: {e}")
        return None


def get_threshold_for_trace(
    client: MoataClient,
    trace_id: int,
    logger: logging.Logger
) -> Optional[float]:
    """
    Get the alarm threshold for a trace.
    
    Args:
        client: Moata API client
        trace_id: Trace ID
        logger: Logger instance
        
    Returns:
        Threshold value if found, None otherwise
    """
    try:
        thresholds = client.alarms.get_thresholds_for_trace(trace_id)
        
        if thresholds:
            # Get the first threshold (usually there's only one for ARI)
            threshold = thresholds[0]
            value = threshold.get("value") or threshold.get("upperLimit")
            if value is not None:
                logger.debug(f"Threshold for trace {trace_id}: {value}")
                return float(value)
        
        logger.warning(f"No threshold found for trace {trace_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting threshold for trace {trace_id}: {e}")
        return None


# =============================================================================
# Alarm Validator
# =============================================================================

def validate_alarm(
    client: MoataClient,
    alarm: AlarmRecord,
    logger: logging.Logger
) -> ValidationResult:
    """
    Validate a single alarm by checking if threshold was exceeded.
    
    Args:
        client: Moata API client
        alarm: Alarm record to validate
        logger: Logger instance
        
    Returns:
        ValidationResult with validation details
    """
    result = ValidationResult(alarm=alarm)
    
    # Step 1: Find the Max TP108 ARI trace
    trace = find_max_tp108_ari_trace(client, alarm.asset_id, logger)
    if not trace:
        result.error = "Max TP108 ARI trace not found"
        return result
    
    result.trace_id = trace.get("id")
    
    # Step 2: Get the threshold
    threshold = get_threshold_for_trace(client, result.trace_id, logger)
    if threshold is None:
        result.error = "Threshold not found"
        return result
    
    result.threshold = threshold
    
    # Step 3: Fetch trace data around the alarm time
    try:
        from_time = (alarm.created_time_utc - timedelta(hours=CHECK_WINDOW_HOURS)).isoformat()
        to_time = (alarm.created_time_utc + timedelta(hours=CHECK_WINDOW_HOURS)).isoformat()
        
        data = client.traces.get_trace_data(
            trace_id=result.trace_id,
            from_time=from_time,
            to_time=to_time,
            data_type="None",
            data_interval=300  # 5-minute resolution
        )
        
        items = data.get("items", [])
        if not items:
            result.error = "No data points in time window"
            return result
        
        # Step 4: Find max value in the window
        values = [item.get("value", 0) for item in items if item.get("value") is not None]
        if not values:
            result.error = "All values are null"
            return result
        
        result.max_value = max(values)
        result.exceeded = result.max_value >= threshold
        
    except Exception as e:
        result.error = str(e)
    
    return result


# =============================================================================
# Main Validation
# =============================================================================

def validate_alarms(
    alarm_file: Path,
    asset_id: Optional[int] = None,
    limit: Optional[int] = None
) -> List[ValidationResult]:
    """
    Validate ARI alarms from Sam's CSV file.
    
    Args:
        alarm_file: Path to alarm CSV file
        asset_id: Optional - validate only this asset
        limit: Optional - limit number of alarms to validate
        
    Returns:
        List of validation results
    """
    logger = logging.getLogger(__name__)
    
    # Load alarms
    logger.info(f"Loading alarms from: {alarm_file}")
    alarms = load_alarm_records(alarm_file)
    logger.info(f"Loaded {len(alarms)} alarm records")
    
    # Filter by asset if specified
    if asset_id:
        alarms = [a for a in alarms if a.asset_id == asset_id]
        logger.info(f"Filtered to {len(alarms)} alarms for asset {asset_id}")
    
    # Apply limit
    if limit:
        alarms = alarms[:limit]
        logger.info(f"Limited to {len(alarms)} alarms")
    
    # Create client
    logger.info("Creating API client...")
    client = create_client()
    
    # Validate each alarm
    results = []
    for i, alarm in enumerate(alarms, 1):
        logger.info(f"\n[{i}/{len(alarms)}] Validating: {alarm.name}")
        logger.info(f"  Alert ID: {alarm.alert_id}")
        logger.info(f"  Time: {alarm.created_time_utc}")
        
        result = validate_alarm(client, alarm, logger)
        results.append(result)
        
        # Log result
        if result.error:
            logger.error(f"  ✗ {result.error}")
        else:
            logger.info(f"  Trace ID: {result.trace_id}")
            logger.info(f"  Threshold: {result.threshold}")
            logger.info(f"  Max Value: {result.max_value:.2f}" if result.max_value else "  Max Value: N/A")
            logger.info(f"  Status: {result.status}")
    
    return results


def print_summary(results: List[ValidationResult], logger: logging.Logger) -> None:
    """Print validation summary."""
    total = len(results)
    valid = sum(1 for r in results if r.exceeded is True)
    invalid = sum(1 for r in results if r.exceeded is False)
    errors = sum(1 for r in results if r.error)
    
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total alarms validated: {total}")
    logger.info(f"  ✓ Valid (threshold exceeded): {valid}")
    logger.info(f"  ✗ Mismatch (not exceeded): {invalid}")
    logger.info(f"  ? Errors: {errors}")
    
    if invalid > 0:
        logger.warning("\nMismatched alarms:")
        for r in results:
            if r.exceeded is False:
                logger.warning(f"  - {r.alarm.name} @ {r.alarm.created_time_utc}")
                logger.warning(f"    Max: {r.max_value:.2f}, Threshold: {r.threshold}")


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate ARI alarms against trace data"
    )
    parser.add_argument(
        "--alarm-file",
        type=Path,
        default=DEFAULT_ALARM_FILE,
        help="Path to alarm CSV file"
    )
    parser.add_argument(
        "--asset-id",
        type=int,
        help="Validate only this asset ID"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of alarms to validate"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup logging
    logger = setup_script_logger(args.log_level)
    
    print_script_header(
        "ARI Alarm Validation",
        __version__,
        logger
    )
    
    logger.info(f"Alarm file: {args.alarm_file}")
    
    try:
        results = validate_alarms(
            alarm_file=args.alarm_file,
            asset_id=args.asset_id,
            limit=args.limit
        )
        
        print_summary(results, logger)
        
        # Exit with error if any mismatches
        mismatches = sum(1 for r in results if r.exceeded is False)
        if mismatches > 0:
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("\nValidation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)
    
    print_script_footer(logger, success=True)


if __name__ == "__main__":
    main()
