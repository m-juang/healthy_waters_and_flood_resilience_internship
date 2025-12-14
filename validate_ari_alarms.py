from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Load .env dari root project
load_dotenv(dotenv_path=Path.cwd() / ".env")

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

ARI_TRACE_DESC = "Max TP108 ARI"
ARI_THRESHOLD = 5.0

WINDOW_HOURS_BEFORE = 24
WINDOW_HOURS_AFTER = 24

DATA_INTERVAL_SECONDS = 300  # 5 minutes (boleh ubah)
DATA_TYPE = "Maximum"        # kita butuh nilai maksimum ARI di window itu


def iso_z(dt: pd.Timestamp) -> str:
    """Convert pandas Timestamp (UTC) -> ISO string with Z."""
    return dt.to_pydatetime().isoformat().replace("+00:00", "Z")


def pick_trace(traces: list[dict], desc_exact: str) -> dict | None:
    target = desc_exact.strip().lower()
    for t in traces:
        if (t.get("description") or "").strip().lower() == target:
            return t
    return None


def extract_gauge_name_from_traces(traces: list[dict]) -> str:
    # Kadang trace punya field asset {name: ...}
    for t in traces:
        asset = t.get("asset") or {}
        name = asset.get("name")
        if name:
            return str(name)
    return "Unknown"


def extract_values(data) -> list[float]:
    # Swagger contoh: {"items":[{"value":...}, ...], ...}
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
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set in .env")

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

    df = pd.read_csv(INPUT_CSV)

    # Pastikan kolom yang dipakai ada
    required_cols = {"assetid", "name", "description", "createdtimeutc"}
    missing = required_cols - set(df.columns.str.lower())
    # Kalau header kamu lowercase semua (assetid, name, description, createdtimeutc) ini aman.
    # Kalau tidak, fallback rename:
    col_map = {c.lower(): c for c in df.columns}
    for need in required_cols:
        if need not in col_map:
            raise RuntimeError(f"Missing column in CSV: {need}")

    asset_col = col_map["assetid"]
    name_col = col_map["name"]
    desc_col = col_map["description"]
    time_col = col_map["createdtimeutc"]

    # filter hanya alarm ARI
    df = df[df[desc_col].astype(str).str.strip() == ARI_TRACE_DESC].copy()

    results: list[dict] = []

    for _, row in df.iterrows():
        asset_id = int(row[asset_col])
        gauge_name_csv = str(row[name_col]) if pd.notna(row[name_col]) else "Unknown"

        alarm_time = pd.to_datetime(row[time_col], utc=True)
        print(f"\nProcessing asset {asset_id} @ {alarm_time} ({gauge_name_csv})")

        # 1) get traces
        try:
            traces = client.get_traces_for_asset(asset_id)
        except Exception as e:
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name_csv,
                "alarm_time": alarm_time,
                "status": "UNVERIFIABLE",
                "reason": f"Failed to fetch traces: {e}",
                "ari_trace_id": None,
                "max_ari_value": None,
                "threshold": ARI_THRESHOLD,
            })
            continue

        # gauge name (prefer from traces if available)
        gauge_name = extract_gauge_name_from_traces(traces)
        if gauge_name == "Unknown":
            gauge_name = gauge_name_csv

        # 2) pick ARI trace
        ari_trace = pick_trace(traces, ARI_TRACE_DESC)
        if not ari_trace:
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time": alarm_time,
                "status": "UNVERIFIABLE",
                "reason": f"ARI trace '{ARI_TRACE_DESC}' not found",
                "ari_trace_id": None,
                "max_ari_value": None,
                "threshold": ARI_THRESHOLD,
            })
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
                    "dataType": DATA_TYPE,             # "Maximum"
                    "dataInterval": DATA_INTERVAL_SECONDS,
                    "padWithZeroes": False,
                },
                allow_404=True,
            )
        except Exception as e:
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time": alarm_time,
                "status": "UNVERIFIABLE",
                "reason": f"Failed to fetch ARI data: {e}",
                "ari_trace_id": ari_trace_id,
                "max_ari_value": None,
                "threshold": ARI_THRESHOLD,
            })
            continue

        values = extract_values(data)
        if not values:
            results.append({
                "assetid": asset_id,
                "gauge_name": gauge_name,
                "alarm_time": alarm_time,
                "status": "UNVERIFIABLE",
                "reason": "No ARI data returned for window",
                "ari_trace_id": ari_trace_id,
                "max_ari_value": None,
                "threshold": ARI_THRESHOLD,
            })
            continue

        max_ari = max(values)

        status = "SUPPORTED" if max_ari >= ARI_THRESHOLD else "NOT_SUPPORTED"

        results.append({
            "assetid": asset_id,
            "gauge_name": gauge_name,
            "alarm_time": alarm_time,
            "status": status,
            "reason": "",
            "ari_trace_id": ari_trace_id,
            "max_ari_value": round(float(max_ari), 6),
            "threshold": ARI_THRESHOLD,
        })

    out_df = pd.DataFrame(results)
    Path("outputs").mkdir(exist_ok=True)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nDONE. Results saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
