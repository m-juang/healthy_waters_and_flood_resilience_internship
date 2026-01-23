from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
import sys

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(dotenv_path=project_root / ".env")

from moata_pipeline.common.constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    TOKEN_TTL_SECONDS,
    TOKEN_REFRESH_BUFFER_SECONDS,
    DEFAULT_REQUESTS_PER_SECOND,
)
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient

# =====================
# SETTINGS
# =====================
# Input alarms (static location)
INPUT_CSV = Path("data/inputs/raingauge_ari_alarms.csv")

# What we are validating
ARI_TRACE_DESC = "Max TP108 ARI"
ARI_THRESHOLD = 5.0

# Data window around the alarm time
WINDOW_HOURS_BEFORE = 1
WINDOW_HOURS_AFTER = 1

# Trace data call
DATA_INTERVAL_SECONDS = 300  # 5 minutes
DATA_TYPE = "None"  # Raw data


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate rain gauge ARI alarms against API data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate.py --date 2026-01-21
  python validate.py --date 2026-01-21 --threshold 10.0

Notes:
  - The --date specifies which analysis folder to use for trace mapping
  - Input alarms are read from data/inputs/raingauge_ari_alarms.csv
  - Output is saved to the validation folder for the specified date
        """
    )
    
    parser.add_argument(
        "--date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Date of analysis folder to use for trace mapping (required)"
    )
    
    parser.add_argument(
        "--threshold",
        type=float,
        default=ARI_THRESHOLD,
        help=f"ARI threshold for validation (default: {ARI_THRESHOLD})"
    )
    
    return parser.parse_args()


def iso_z(dt: pd.Timestamp) -> str:
    """Convert pandas Timestamp (UTC) -> ISO string with Z."""
    if dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_trace_mapping(csv_path: Path) -> dict[int, int]:
    """
    Build mapping: asset_id -> trace_id for Max TP108 ARI traces.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Trace mapping file not found: {csv_path}\n\n"
            f"Have you run the analysis pipeline for this date?\n"
            f"  python scripts/gauge/analyze.py --date YYYY-MM-DD"
        )
    
    df = pd.read_csv(csv_path)
    ari = df[df["trace_description"] == ARI_TRACE_DESC].copy()
    ari = ari.dropna(subset=["gauge_id", "trace_id"])
    
    mapping = {}
    for _, row in ari.iterrows():
        asset_id = int(row["gauge_id"])
        trace_id = int(row["trace_id"])
        mapping[asset_id] = trace_id
    
    return mapping


def main() -> None:
    # Parse arguments
    args = parse_args()
    
    # Setup paths based on date
    paths = PipelinePaths.for_date(args.date)
    
    TRACE_MAPPING_CSV = paths.alarm_summary_full_csv
    OUTPUT_CSV = paths.rain_gauges_validation_dir / "ari_alarm_validation.csv"
    
    print("=" * 60)
    print("Rain Gauge ARI Alarm Validation")
    print("=" * 60)
    print(f"Date:            {args.date}")
    print(f"Input alarms:    {INPUT_CSV}")
    print(f"Trace mapping:   {TRACE_MAPPING_CSV}")
    print(f"Output:          {OUTPUT_CSV}")
    print(f"ARI Threshold:   {args.threshold}")
    print("=" * 60)
    print()
    
    # Check input files exist
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"Input alarm file not found: {INPUT_CSV}\n"
            f"Please place your alarm CSV in data/inputs/"
        )
    
    # --- credentials ---
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set in .env")

    # --- auth + http + client ---
    auth = MoataAuth(
        token_url=TOKEN_URL,
        scope=OAUTH_SCOPE,
        client_id=client_id,
        client_secret=client_secret,
        verify_ssl=False,
        ttl_seconds=TOKEN_TTL_SECONDS,
        refresh_buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS,
    )

    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        requests_per_second=DEFAULT_REQUESTS_PER_SECOND,
        verify_ssl=False,
    )

    client = MoataClient(http=http)
    print("✓ Client ready")

    # --- build trace mapping from our analyzed data ---
    print(f"Loading trace mapping from {TRACE_MAPPING_CSV}...")
    asset_to_trace = build_trace_mapping(TRACE_MAPPING_CSV)
    print(f"✓ Found {len(asset_to_trace)} gauges with {ARI_TRACE_DESC} traces")

    # --- load input alarms ---
    print(f"Loading alarm events from {INPUT_CSV}...")
    alarms_df = pd.read_csv(INPUT_CSV)
    print(f"✓ Loaded {len(alarms_df)} alarm events")

    results: list[dict] = []

    for idx, row in alarms_df.iterrows():
        asset_id = int(row["assetid"])
        gauge_name = str(row["name"])
        alarm_time = pd.to_datetime(row["createdtimeutc"], utc=True)

        print(f"\n[{idx+1}/{len(alarms_df)}] {gauge_name}")
        print(f"  Alarm time: {alarm_time}")

        # Get trace_id from our mapping
        trace_id = asset_to_trace.get(asset_id)
        if not trace_id:
            print(f"  ⚠ No trace mapping found for asset {asset_id}")
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time_utc": alarm_time,
                "trace_id": None,
                "status": "UNVALIDATABLE",
                "reason": "No trace mapping found",
                "max_ari_value": None,
                "threshold": args.threshold,
            })
            continue

        # Fetch data around alarm time
        from_time = iso_z(alarm_time - timedelta(hours=WINDOW_HOURS_BEFORE))
        to_time = iso_z(alarm_time + timedelta(hours=WINDOW_HOURS_AFTER))

        try:
            data = client.get_trace_data(
                trace_id=trace_id,
                from_time=from_time,
                to_time=to_time,
                data_type=DATA_TYPE,
                data_interval=DATA_INTERVAL_SECONDS,
            )
        except Exception as e:
            print(f"  ⚠ Failed to fetch data: {e}")
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time_utc": alarm_time,
                "trace_id": trace_id,
                "status": "UNVALIDATABLE",
                "reason": f"API error: {e}",
                "max_ari_value": None,
                "threshold": args.threshold,
            })
            continue

        items = data.get("items", [])
        if not items:
            print(f"  ⚠ No data returned")
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time_utc": alarm_time,
                "trace_id": trace_id,
                "status": "UNVALIDATABLE",
                "reason": "No data in window",
                "max_ari_value": None,
                "threshold": args.threshold,
            })
            continue

        # Find max value
        values = [item.get("value", 0) for item in items]
        max_value = max(values) if values else 0

        # Check if threshold was exceeded
        exceeded = max_value >= args.threshold
        status = "VALIDATED" if exceeded else "NOT_VALIDATED"

        print(f"  Trace ID: {trace_id}")
        print(f"  Max ARI value: {max_value:.2f}")
        print(f"  Threshold: {args.threshold}")
        print(f"  Status: {status}")

        results.append({
            "assetid": asset_id,
            "gauge_name": gauge_name,
            "alarm_time_utc": alarm_time,
            "trace_id": trace_id,
            "status": status,
            "reason": "",
            "max_ari_value": round(max_value, 2),
            "threshold": args.threshold,
        })

    # Save results
    out_df = pd.DataFrame(results)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT_CSV, index=False)

    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(out_df["status"].value_counts().to_string())
    print(f"\nResults saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()