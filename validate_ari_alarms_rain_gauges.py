from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

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

# =====================
# SETTINGS
# =====================
INPUT_CSV = Path("data/inputs/raingauge_ari_alarms.csv")
TRACE_MAPPING_CSV = Path("outputs/rain_gauges/analyze/alarm_summary_full.csv")
OUTPUT_CSV = Path("outputs/rain_gauges/ari_alarm_validation.csv")

# What we are validating
ARI_TRACE_DESC = "Max TP108 ARI"
ARI_THRESHOLD = 5.0

# Data window around the alarm time
WINDOW_HOURS_BEFORE = 1
WINDOW_HOURS_AFTER = 1

# Trace data call
DATA_INTERVAL_SECONDS = 300  # 5 minutes
DATA_TYPE = "None"  # Raw data


def iso_z(dt: pd.Timestamp) -> str:
    """Convert pandas Timestamp (UTC) -> ISO string with Z."""
    if dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_trace_mapping(csv_path: Path) -> dict[int, int]:
    """
    Build mapping: asset_id -> trace_id for Max TP108 ARI traces.
    """
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
                "status": "UNVERIFIABLE",
                "reason": "No trace mapping found",
                "max_ari_value": None,
                "threshold": ARI_THRESHOLD,
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
                "status": "UNVERIFIABLE",
                "reason": f"API error: {e}",
                "max_ari_value": None,
                "threshold": ARI_THRESHOLD,
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
                "status": "UNVERIFIABLE",
                "reason": "No data in window",
                "max_ari_value": None,
                "threshold": ARI_THRESHOLD,
            })
            continue

        # Find max value
        values = [item.get("value", 0) for item in items]
        max_value = max(values) if values else 0

        # Check if threshold was exceeded
        exceeded = max_value >= ARI_THRESHOLD
        status = "VERIFIED" if exceeded else "NOT_VERIFIED"

        print(f"  Trace ID: {trace_id}")
        print(f"  Max ARI value: {max_value:.2f}")
        print(f"  Threshold: {ARI_THRESHOLD}")
        print(f"  Status: {status}")

        results.append({
            "assetid": asset_id,
            "gauge_name": gauge_name,
            "alarm_time_utc": alarm_time,
            "trace_id": trace_id,
            "status": status,
            "reason": "",
            "max_ari_value": round(max_value, 2),
            "threshold": ARI_THRESHOLD,
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