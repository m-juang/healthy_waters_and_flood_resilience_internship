#!/usr/bin/env python3
"""
Test Script: Verify Swanson Rainfall Data

Checks if ACC - Rain - Swanson @ Waitakere Filter Station had 100mm 
in the last 24 hours ending 2026-01-21 22:20:00 UTC.

Usage:
    python test_swanson_rainfall.py
"""

import os
from datetime import datetime, timedelta, timezone
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from dotenv import load_dotenv
load_dotenv()

from moata_pipeline.common.constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    DEFAULT_PROJECT_ID,
)
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient


def main():
    print("=" * 70)
    print("Test: ACC - Rain - Swanson @ Waitakere Filter Station")
    print("=" * 70)
    
    # Target parameters - check 100mm in last 24 hours ending at 22:20:00 NZDT (09:20 UTC)
    gauge_name = "ACC - Rain - Swanson @ Waitakere Filter Station"
    end_time = datetime(2026, 1, 21, 9, 20, 0, tzinfo=timezone.utc)  # 22:20 NZDT = 09:20 UTC
    start_time = end_time - timedelta(hours=24)  # Last 24 hours
    expected_mm = 100
    
    print(f"\nTarget Gauge: {gauge_name}")
    print(f"Time Range: {start_time} to {end_time} UTC")
    print(f"  (NZDT: {(start_time + timedelta(hours=13)).strftime('%Y-%m-%d %H:%M')} to {(end_time + timedelta(hours=13)).strftime('%Y-%m-%d %H:%M')})")
    print(f"Expected: ~{expected_mm}mm in 24 hours")
    print()
    
    # Setup API client
    print("Connecting to Moata API...")
    auth = MoataAuth(
        token_url=TOKEN_URL,
        client_id=os.getenv("MOATA_CLIENT_ID"),
        client_secret=os.getenv("MOATA_CLIENT_SECRET"),
        scope=OAUTH_SCOPE,
        verify_ssl=False,
    )
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        verify_ssl=False,
    )
    client = MoataClient(http=http)
    print("✓ Connected\n")
    
    # Step 1: Find the gauge
    print("Step 1: Finding gauge...")
    gauges = client.get_rain_gauges(
        project_id=DEFAULT_PROJECT_ID,
        asset_type_id=100
    )
    
    # Search for Swanson gauge
    target_gauge = None
    for g in gauges:
        name = g.get("name", "").lower()
        if "swanson" in name and "waitakere" in name:
            target_gauge = g
            break
    
    if not target_gauge:
        print("✗ Gauge not found in rain gauges list")
        return
    
    asset_id = target_gauge.get("id")
    print(f"✓ Found: {target_gauge.get('name')}")
    print(f"  Asset ID: {asset_id}")
    print()
    
    # Step 2: Get traces for this asset
    print("Step 2: Getting traces...")
    traces = client.get_traces_for_asset(asset_id)
    print(f"✓ Found {len(traces)} traces")
    
    # Find rainfall trace (usually has "Rainfall" in description)
    rainfall_trace = None
    print("\nAvailable traces:")
    for t in traces:
        desc = t.get("description", "")
        name = t.get("name", "")
        print(f"  - ID {t.get('id')}: {name} | {desc}")
    
    print()
    
    for t in traces:
        desc = t.get("description", "").lower()
        name = t.get("name", "").lower()
        # Look for the main rainfall trace - typically just "Rainfall" without window/sum/ari
        if desc == "rainfall" or (desc == "rainfall" and "window" not in name and "sum" not in name):
            rainfall_trace = t
            break
        # Alternative: look for raw rainfall trace
        if "rainfall" in desc and "window" not in desc and "sum" not in desc and "ari" not in desc:
            if rainfall_trace is None:
                rainfall_trace = t
    
    if rainfall_trace:
        print(f"  → Selected trace: {rainfall_trace.get('name')} (ID: {rainfall_trace.get('id')})")
        print(f"    Description: {rainfall_trace.get('description')}")
    
    if not rainfall_trace:
        print("✗ Rainfall trace not found")
        print("\nAvailable traces:")
        for t in traces:
            print(f"  - {t.get('name')}: {t.get('description')}")
        return
    
    trace_id = rainfall_trace.get("id")
    print()
    
    # Step 3: Get rainfall data
    print("Step 3: Fetching rainfall data (last 24 hours)...")
    
    from_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Get raw data (Total aggregation for rainfall)
    data = client.get_trace_data(
        trace_id=trace_id,
        from_time=from_time,
        to_time=to_time,
        data_type="Total",  # Rainfall requires Total aggregation
        data_interval=60,  # 1 minute intervals for detail
    )
    
    items = data.get("items", [])
    if not items:
        # Try with data.data.items structure
        if "data" in data and "items" in data["data"]:
            items = data["data"]["items"]
    
    print(f"✓ Retrieved {len(items)} data points")
    
    # Debug: show first few items structure
    if items:
        print(f"\nSample data point structure: {items[0]}")
    print()
    
    # Step 4: Calculate total rainfall
    print("Step 4: Calculating total rainfall...")
    print("-" * 60)
    
    total_rainfall = 0.0
    hourly_totals = {}
    
    for item in items:
        # Handle different response formats
        unix_ts = item.get("whenRecordedUnixSeconds")
        if unix_ts:
            dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
            time_str = dt.strftime("%Y-%m-%d %H:%M")
            hour_key = dt.strftime("%Y-%m-%d %H:00")
        else:
            time_str = item.get("time", item.get("t", "N/A"))
            hour_key = time_str[:13] if time_str and len(time_str) >= 13 else "Unknown"
        
        value = item.get("value", item.get("v", 0)) or 0  # Handle None
        total_rainfall += value
        
        # Group by hour for summary
        hourly_totals[hour_key] = hourly_totals.get(hour_key, 0) + value
    
    # Show hourly summary
    print(f"{'Hour (UTC)':<25} {'Rainfall (mm)':<15}")
    print("-" * 60)
    
    for hour, total in sorted(hourly_totals.items()):
        print(f"{hour:<25} {total:.2f}")
    
    print("-" * 60)
    print(f"{'TOTAL (12 hours)':<25} {total_rainfall:.2f} mm")
    print("=" * 70)
    
    # Step 5: Verification
    print("\n" + "=" * 70)
    print("VERIFICATION RESULT")
    print("=" * 70)
    print(f"Gauge: {target_gauge.get('name')}")
    print(f"Period: {start_time} to {end_time} UTC")
    print(f"  (NZDT: {(start_time + timedelta(hours=13)).strftime('%Y-%m-%d %H:%M')} to {(end_time + timedelta(hours=13)).strftime('%Y-%m-%d %H:%M')})")
    print(f"Total Rainfall: {total_rainfall:.2f} mm")
    print(f"Expected: ~{expected_mm} mm")
    print()
    
    if total_rainfall >= expected_mm - 5 and total_rainfall <= expected_mm + 5:
        print(f"✓ CONFIRMED: Approximately {expected_mm}mm in 12 hours")
    elif total_rainfall >= expected_mm - 10:
        print(f"⚠ CLOSE: {total_rainfall:.1f}mm (expected ~{expected_mm}mm)")
    else:
        print(f"✗ NOT MATCHED: Only {total_rainfall:.1f}mm recorded")
    
    print("=" * 70)
    
    # Bonus: Check if there were any alarms
    print("\nBonus: Checking for alarms on this trace...")
    try:
        alarms = client.get_alarms_for_trace(trace_id)
        if alarms:
            print(f"Found {len(alarms)} alarm(s):")
            for a in alarms[:5]:  # Show first 5
                print(f"  - {a.get('alarmTime')}: {a.get('value')} (Threshold: {a.get('threshold')})")
        else:
            print("No alarms found for this trace")
    except Exception as e:
        print(f"Could not fetch alarms: {e}")
    
    # Step 4b: List alarms for all window hours
    print("\nStep 4b: Checking alarms for all window hours (1, 2, 6, 12, 24)...")
    from scripts.gauge.check_alarms import fetch_rainfall_total, get_alarm_thresholds
    window_hours_list = [1, 2, 6, 12, 24]
    thresholds = get_alarm_thresholds(client, trace_id)
    alarms_found = []
    for hours in window_hours_list:
        total_mm, count = fetch_rainfall_total(client, trace_id, end_time, hours)
        threshold = thresholds.get(hours, 0)
        if total_mm >= threshold and threshold > 0:
            alarms_found.append((hours, total_mm, threshold))
            print(f"🚨 ALARM: Swanson {total_mm:.1f}mm in the last {hours} hours (threshold {threshold}mm), {end_time}")
        else:
            print(f"No alarm: Swanson {total_mm:.1f}mm in the last {hours} hours (threshold {threshold}mm), {end_time}")
    if alarms_found:
        print("\nSummary of detected alarms:")
        for hours, total_mm, threshold in alarms_found:
            print(f"  - {total_mm:.1f}mm in {hours}h (threshold {threshold}mm)")
    else:
        print("\nNo alarms detected in any window.")
    print("\nSelesai.")


if __name__ == "__main__":
    main()
