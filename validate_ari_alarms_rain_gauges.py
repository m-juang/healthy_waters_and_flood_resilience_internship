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
INPUT_CSV = "data/inputs/raingauge_ari_alarms.csv"
OUTPUT_CSV = "outputs/ari_alarm_validation_by_ari_trace.csv"

# What we are validating
ARI_TRACE_DESC = "Max TP108 ARI"
ARI_THRESHOLD = 5.0

# Data window around the alarm time
WINDOW_HOURS_BEFORE = 24
WINDOW_HOURS_AFTER = 24

# Trace data call
DATA_INTERVAL_SECONDS = 300  # 5 minutes
DATA_TYPE = "Maximum"        # as per your comment


def iso_z(dt: pd.Timestamp) -> str:
    """Convert pandas Timestamp (UTC) -> ISO string with Z."""
    # Ensure timezone aware
    if dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    return dt.to_pydatetime().isoformat().replace("+00:00", "Z")


def pick_trace(traces: list[dict], desc_key: str) -> dict | None:
    """
    Find a trace by description. First try exact match, then substring match.
    """
    target = desc_key.strip().lower()

    # 1) exact match
    for t in traces:
        if (t.get("description") or "").strip().lower() == target:
            return t

    # 2) contains
    for t in traces:
        if target in ((t.get("description") or "").strip().lower()):
            return t

    return None


def extract_gauge_name_from_traces(traces: list[dict]) -> str:
    """Try to get asset.name from any trace payload."""
    for t in traces:
        asset = t.get("asset") or {}
        name = asset.get("name")
        if name:
            return str(name)
    return "Unknown"


def extract_values(data) -> list[float]:
    """
    Expected swagger shape: {"items":[{"value":...}, ...], ...}
    Also tolerate a plain list.
    """
    if data is None:
        return []

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("items", [])
    else:
        return []

    vals: list[float] = []
    for it in items:
        v = it.get("value") if isinstance(it, dict) else None
        if v is None:
            continue
        try:
            vals.append(float(v))
        except Exception:
            continue
    return vals


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
    print("Client ready")

    # --- load input ---
    df = pd.read_csv(INPUT_CSV)

    # Normalize column mapping (case-insensitive)
    required_cols = {"assetid", "name", "description", "createdtimeutc"}
    col_map = {c.lower(): c for c in df.columns}

    missing = sorted([c for c in required_cols if c not in col_map])
    if missing:
        raise RuntimeError(f"Missing column(s) in CSV: {missing}")

    asset_col = col_map["assetid"]
    name_col = col_map["name"]
    desc_col = col_map["description"]
    time_col = col_map["createdtimeutc"]

    # Filter only ARI alarms by description (case-insensitive)
    df = df[df[desc_col].astype(str).str.strip().str.lower() == ARI_TRACE_DESC.strip().lower()].copy()

    results: list[dict] = []

    for _, row in df.iterrows():
        # 0) row inputs
        asset_id = int(row[asset_col])
        gauge_name_csv = str(row[name_col]) if pd.notna(row[name_col]) else "Unknown"
        alarm_time = pd.to_datetime(row[time_col], utc=True)

        print(f"\nProcessing asset {asset_id} @ {alarm_time} ({gauge_name_csv})")

        # 1) get traces for the asset
        try:
            traces = client.get_traces_for_asset(asset_id)
        except Exception as e:
            results.append(
                {
                    "assetid": asset_id,
                    "gauge_name": gauge_name_csv,
                    "alarm_time": alarm_time,
                    "status": "UNVERIFIABLE",
                    "reason": f"Failed to fetch traces: {e}",
                    "ari_trace_id": None,
                    "max_ari_value": None,
                    "threshold": ARI_THRESHOLD,
                }
            )
            continue

        # Prefer gauge name from traces if present
        gauge_name = extract_gauge_name_from_traces(traces)
        if gauge_name == "Unknown":
            gauge_name = gauge_name_csv

        # 2) pick ARI trace by description
        ari_trace = pick_trace(traces, ARI_TRACE_DESC)
        if not ari_trace:
            results.append(
                {
                    "assetid": asset_id,
                    "gauge_name": gauge_name,
                    "alarm_time": alarm_time,
                    "status": "UNVERIFIABLE",
                    "reason": f"ARI trace '{ARI_TRACE_DESC}' not found",
                    "ari_trace_id": None,
                    "max_ari_value": None,
                    "threshold": ARI_THRESHOLD,
                }
            )
            continue

        ari_trace_id = ari_trace.get("id")

        # 3) fetch ARI data around alarm time
        from_time = iso_z(alarm_time - timedelta(hours=WINDOW_HOURS_BEFORE))
        to_time = iso_z(alarm_time + timedelta(hours=WINDOW_HOURS_AFTER))

        try:
            data = http.get(
                f"traces/{ari_trace_id}/data/utc",
                params={
                    "from": from_time,
                    "to": to_time,
                    "dataType": DATA_TYPE,  # "Maximum"
                    "dataInterval": DATA_INTERVAL_SECONDS,
                    "padWithZeroes": False,
                },
                allow_404=True,
            )
        except Exception as e:
            results.append(
                {
                    "assetid": asset_id,
                    "gauge_name": gauge_name,
                    "alarm_time": alarm_time,
                    "status": "UNVERIFIABLE",
                    "reason": f"Failed to fetch ARI data: {e}",
                    "ari_trace_id": ari_trace_id,
                    "max_ari_value": None,
                    "threshold": ARI_THRESHOLD,
                }
            )
            continue

        values = extract_values(data)
        if not values:
            results.append(
                {
                    "assetid": asset_id,
                    "gauge_name": gauge_name,
                    "alarm_time": alarm_time,
                    "status": "UNVERIFIABLE",
                    "reason": "No ARI data returned for window",
                    "ari_trace_id": ari_trace_id,
                    "max_ari_value": None,
                    "threshold": ARI_THRESHOLD,
                }
            )
            continue

        max_ari = max(values)
        status = "SUPPORTED" if max_ari >= ARI_THRESHOLD else "NOT_SUPPORTED"

        results.append(
            {
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time": alarm_time,
                "status": status,
                "reason": "",
                "ari_trace_id": ari_trace_id,
                "max_ari_value": round(float(max_ari), 6),
                "threshold": ARI_THRESHOLD,
            }
        )

    out_df = pd.DataFrame(results)

    out_path = Path(OUTPUT_CSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)

    print(f"\nDONE. Results saved to {out_path}")


if __name__ == "__main__":
    main()