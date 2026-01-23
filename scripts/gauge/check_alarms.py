"""
Check Alarms Script

Fetches all rain gauge alarms in the last 24 hours and verifies rainfall data.
Outputs formatted alarm list with actual rainfall values.

Usage:
    python check_alarms.py --datetime "2026-01-21 22:40"
    python check_alarms.py  # Uses current time

Output Format:
    Rainfall Depth: ACC - Rain - Swanson @ Waitakere 100mm in the last 24 hours, 2026-01-21 22:20:00

Author: Auckland Council Internship Team
"""

from __future__ import annotations

import argparse
import json
import os

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
from dotenv import load_dotenv

# Setup paths
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(dotenv_path=project_root / ".env")

from moata_pipeline.common.constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    DEFAULT_PROJECT_ID,
)
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient


# =============================================================================
# CONFIGURATION
# =============================================================================

LOOKBACK_HOURS = 24  # Check alarms in last 24 hours
NZDT_OFFSET = 13  # NZDT = UTC + 13 hours

# Alarm windows to check (minutes/hours, description)
ALARM_WINDOWS = [
    (0.5, "30 minutes"),
    (1, "60 minutes"),
    (6, "6 hours"),
    (12, "12 hours"),
    (24, "24 hours"),
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_datetime_arg(dt_str: str) -> datetime:
    """Parse datetime string to UTC datetime."""
    # Parse as NZDT, convert to UTC
    local_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
    utc_dt = local_dt.replace(tzinfo=timezone.utc) - timedelta(hours=NZDT_OFFSET)
    return utc_dt


def to_nzdt(utc_dt: datetime) -> datetime:
    """Convert UTC datetime to NZDT."""
    return utc_dt + timedelta(hours=NZDT_OFFSET)


def format_nzdt(utc_dt: datetime) -> str:
    """Format UTC datetime as NZDT string."""
    nzdt = to_nzdt(utc_dt)
    return nzdt.strftime("%Y-%m-%d %H:%M:%S")


def get_rainfall_trace(client: MoataClient, asset_id: int) -> Optional[Dict]:
    """Find the main Rainfall trace for an asset."""
    traces = client.get_traces_for_asset(asset_id)
    
    for t in traces:
        desc = t.get("description", "").lower()
        if desc == "rainfall":
            return t
    
    # Fallback: look for rainfall without window/sum
    for t in traces:
        desc = t.get("description", "").lower()
        name = t.get("name", "").lower()
        if "rainfall" in desc and "window" not in desc and "sum" not in desc and "ari" not in desc:
            return t
    
    return None




def fetch_rainfall_total(
    client: MoataClient,
    trace_id: int,
    end_time: datetime,
    hours: int
) -> Tuple[float, int]:
    """
    Fetch rainfall total for a trace in given time window.
    Returns:
        Tuple of (total_mm, data_point_count)
    """
    # Support fractional hours (e.g., 0.5 for 30 minutes)
    start_time = end_time - timedelta(hours=hours)
    from_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Use 30min interval for 30min window, 60min for 60min window, else 60min
    data_interval = 30 if hours == 0.5 else 60
    result = client.get_trace_data(
        trace_id=trace_id,
        from_time=from_str,
        to_time=to_str,
        data_type="Total",
        data_interval=data_interval,
    )
    items = result.get("items", [])
    total = sum(item.get("value", 0) or 0 for item in items)
    return total, len(items)


def find_best_alarm_window(
    client: MoataClient,
    trace_id: int,
    end_time: datetime,
    thresholds: Dict[int, float]
) -> Optional[Dict]:
    """
    Find the alarm window that best matches (closest to threshold).
    
    Args:
        client: Moata client
        trace_id: Trace ID
        end_time: End time for calculation
        thresholds: Dict mapping hours -> threshold mm
        
    Returns:
        Dict with hours, mm, threshold info if alarm found, else None
    """
    results = []
    
    for hours, desc in ALARM_WINDOWS:
        total_mm, count = fetch_rainfall_total(client, trace_id, end_time, hours)
        threshold = thresholds.get(hours, 0)
        
        if total_mm >= threshold and threshold > 0:
            results.append({
                "hours": hours,
                "description": desc,
                "total_mm": total_mm,
                "threshold": threshold,
                "data_points": count,
                "exceeds_by": total_mm - threshold,
            })
    
    if results:
        # Return the one with smallest time window that exceeds threshold
        return min(results, key=lambda x: x["hours"])
    
    return None


def get_alarm_thresholds(client: MoataClient, trace_id: int) -> Dict[int, float]:
    """
    Get alarm thresholds from trace configuration.
    
    Returns dict mapping window_hours -> threshold_mm
    """
    # This is a simplified version - actual thresholds would come from alarm config
    # For now, we'll use typical Auckland Council thresholds
    return {
        0.5: 10,   # 10mm in 30 minutes (example, adjust as needed)
        1: 15,     # 15mm in 60 minutes (example, adjust as needed)
        6: 50,     # 50mm in 6 hours
        12: 70,    # 70mm in 12 hours
        24: 100,   # 100mm in 24 hours
    }


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def check_all_gauges(
    client: MoataClient,
    end_time_utc: datetime,
    output_dir: Path
) -> List[Dict]:
    """
    Check all rain gauges for alarms in the last 24 hours.
    
    Returns list of alarm records.
    """
    print(f"\n{'='*70}")
    print("RAIN GAUGE ALARM CHECK")
    print(f"{'='*70}")
    print(f"End Time (UTC): {end_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time (NZDT): {format_nzdt(end_time_utc)}")
    print(f"Lookback: {LOOKBACK_HOURS} hours")
    print(f"{'='*70}\n")
    
    # Get all rain gauges
    print("Fetching rain gauges...")
    gauges = client.get_rain_gauges(project_id=DEFAULT_PROJECT_ID, asset_type_id=100)
    print(f"✓ Found {len(gauges)} rain gauges\n")
    
    alarms_found = []
    gauges_checked = 0

    # Batching: fetch all traces for all gauges at once
    asset_ids = [g.get("id") for g in gauges]
    traces_by_asset = client.get_traces_for_assets(asset_ids)

    alarms_found = []
    for i, gauge in enumerate(gauges, 1):
        asset_id = gauge.get("id")
        gauge_name = gauge.get("name", f"Unknown ({asset_id})")
        print(f"[{i}/{len(gauges)}] Checking gauge: {gauge_name} (ID: {asset_id})")
        traces = traces_by_asset.get(asset_id, [])
        rainfall_trace = None
        for t in traces:
            desc = t.get("description", "").lower()
            if desc == "rainfall":
                rainfall_trace = t
                break
        if not rainfall_trace:
            for t in traces:
                desc = t.get("description", "").lower()
                name = t.get("name", "").lower()
                if "rainfall" in desc and "window" not in desc and "sum" not in desc and "ari" not in desc:
                    rainfall_trace = t
                    break
        if not rainfall_trace:
            print(f"    [!] No rainfall trace found for {gauge_name}")
            continue
        trace_id = rainfall_trace.get("id")
        thresholds = get_alarm_thresholds(client, trace_id)
        for hours, window_desc in ALARM_WINDOWS:
            if hours == 0.5:
                label = "30min"
            elif hours == 1:
                label = "60min"
            else:
                label = f"{int(hours)}h"
            print(f"      - Checking window: {label} ({window_desc})...")
            try:
                total_mm, count = fetch_rainfall_total(client, trace_id, end_time_utc, hours)
            except Exception as e:
                print(f"        [!] Error fetching {gauge_name} {label}: {e}")
                continue
            threshold = thresholds.get(hours, 0)
            if count == 0:
                print(f"        [!] No data points for {gauge_name} {label}")
                continue
            if total_mm >= threshold and threshold > 0:
                alarm_record = {
                    "gauge_name": gauge_name,
                    "asset_id": asset_id,
                    "trace_id": trace_id,
                    "hours": hours,
                    "window_description": window_desc,
                    "total_mm": round(total_mm, 2),
                    "threshold_mm": threshold,
                    "end_time_utc": end_time_utc.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time_nzdt": format_nzdt(end_time_utc),
                    "data_points": count,
                }
                alarms_found.append(alarm_record)
                print(f"        🚨 ALARM: {gauge_name} {total_mm:.1f}mm in the last {window_desc} (threshold {threshold}mm)")
    alarms = alarms_found
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate text report
    report_lines = [
        "=" * 70,
        "RAIN GAUGE ALARM REPORT",
        "=" * 70,
        f"Generated: {format_nzdt(datetime.now(timezone.utc))}",
        f"Report Period End: {format_nzdt(end_time_utc)}",
        f"Total Alarms: {len(alarms)}",
        "=" * 70,
        "",
    ]
    
    if not alarms:
        report_lines.append("No alarms detected in the last 24 hours.")
    else:
        # Sort by total_mm descending
        sorted_alarms = sorted(alarms, key=lambda x: x["total_mm"], reverse=True)
        
        for i, alarm in enumerate(sorted_alarms, 1):
            # Format like: Rainfall Depth: ACC - Rain - Swanson @ Waitakere 100mm in the last 24 hours, 2026-01-21 22:20:00
            line = (
                f"Rainfall Depth: {alarm['gauge_name']} "
                f"{alarm['total_mm']:.0f}mm in the last {alarm['window_description']}, "
                f"{alarm['end_time_nzdt']}."
            )
            report_lines.append(f"{i:3d}. {line}")
    
    report_lines.extend(["", "=" * 70])
    
    # Write text report
    report_txt = output_dir / "alarm_report.txt"
    report_txt.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"✓ Text report saved: {report_txt}")
    
    # Write CSV
    if alarms:
        df = pd.DataFrame(alarms)
        report_csv = output_dir / "alarm_report.csv"
        df.to_csv(report_csv, index=False)
        print(f"✓ CSV report saved: {report_csv}")
    
    # Write JSON
    report_json = output_dir / "alarm_report.json"
    with open(report_json, "w", encoding="utf-8") as f:
        json.dump({
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "generated_nzdt": format_nzdt(datetime.now(timezone.utc)),
            "report_end_time_utc": end_time_utc.isoformat(),
            "report_end_time_nzdt": format_nzdt(end_time_utc),
            "total_alarms": len(alarms),
            "alarms": alarms,
        }, f, indent=2)
    print(f"✓ JSON report saved: {report_json}")
    
    return alarms


def generate_html_dashboard(alarms: List[Dict], end_time_utc: datetime, output_dir: Path) -> Path:
    """Generate HTML visualization dashboard."""
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Sort alarms by rainfall amount
    sorted_alarms = sorted(alarms, key=lambda x: x["total_mm"], reverse=True)
    
    # Generate alarm rows
    alarm_rows = ""
    for alarm in sorted_alarms:
        severity_class = "critical" if alarm["total_mm"] >= 100 else "warning" if alarm["total_mm"] >= 50 else "normal"
        alarm_rows += f"""
        <tr class="{severity_class}">
            <td>{alarm['gauge_name']}</td>
            <td class="rainfall">{alarm['total_mm']:.1f} mm</td>
            <td>{alarm['window_description']}</td>
            <td>{alarm['threshold_mm']} mm</td>
            <td>{alarm['end_time_nzdt']}</td>
        </tr>
        """
    
    # HTML template
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rain Gauge Alarm Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            color: #fff;
            padding: 20px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: rgba(255,255,255,0.1);
            border-radius: 15px;
        }}
        h1 {{
            font-size: 2.5rem;
            margin-bottom: 10px;
            background: linear-gradient(90deg, #00d4ff, #00ff88);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .meta {{
            color: #888;
            font-size: 0.9rem;
        }}
        .summary {{
            display: flex;
            gap: 20px;
            margin-bottom: 30px;
            flex-wrap: wrap;
        }}
        .stat-card {{
            background: rgba(255,255,255,0.1);
            padding: 20px 30px;
            border-radius: 10px;
            flex: 1;
            min-width: 200px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 2.5rem;
            font-weight: bold;
            color: #00ff88;
        }}
        .stat-label {{
            color: #888;
            margin-top: 5px;
        }}
        .alarms-section {{
            background: rgba(255,255,255,0.05);
            border-radius: 15px;
            padding: 20px;
            overflow-x: auto;
        }}
        h2 {{
            margin-bottom: 20px;
            color: #00d4ff;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }}
        th {{
            background: rgba(0,212,255,0.2);
            color: #00d4ff;
            font-weight: 600;
        }}
        tr:hover {{
            background: rgba(255,255,255,0.05);
        }}
        .critical {{
            background: rgba(255,0,0,0.15);
        }}
        .critical .rainfall {{
            color: #ff4444;
            font-weight: bold;
        }}
        .warning {{
            background: rgba(255,165,0,0.1);
        }}
        .warning .rainfall {{
            color: #ffa500;
            font-weight: bold;
        }}
        .rainfall {{
            font-weight: 600;
            color: #00ff88;
        }}
        .no-alarms {{
            text-align: center;
            padding: 50px;
            color: #00ff88;
            font-size: 1.5rem;
        }}
        footer {{
            text-align: center;
            margin-top: 30px;
            color: #666;
            font-size: 0.8rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🌧️ Rain Gauge Alarm Dashboard</h1>
            <p class="meta">
                Report End Time: {format_nzdt(end_time_utc)} (NZDT)<br>
                Generated: {format_nzdt(datetime.now(timezone.utc))}
            </p>
        </header>
        
        <div class="summary">
            <div class="stat-card">
                <div class="stat-value">{len(alarms)}</div>
                <div class="stat-label">Total Alarms</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len([a for a in alarms if a['total_mm'] >= 100])}</div>
                <div class="stat-label">Critical (≥100mm)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len([a for a in alarms if 50 <= a['total_mm'] < 100])}</div>
                <div class="stat-label">Warning (50-99mm)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{max([a['total_mm'] for a in alarms]) if alarms else 0:.0f}mm</div>
                <div class="stat-label">Max Rainfall</div>
            </div>
        </div>
        
        <div class="alarms-section">
            <h2>📋 Alarm Details</h2>
            {"<table><thead><tr><th>Gauge Name</th><th>Rainfall</th><th>Period</th><th>Threshold</th><th>Time (NZDT)</th></tr></thead><tbody>" + alarm_rows + "</tbody></table>" if alarms else "<div class='no-alarms'>✅ No alarms in the last 24 hours</div>"}
        </div>
        
        <footer>
            Rain Gauge Alarm Check - Auckland Council Moata Pipeline
        </footer>
    </div>
</body>
</html>
"""
    
    dashboard_path = output_dir / "alarm_dashboard.html"
    dashboard_path.write_text(html_content, encoding="utf-8")
    print(f"✓ HTML dashboard saved: {dashboard_path}")
    
    return dashboard_path


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Check rain gauge alarms in last 24 hours")
    parser.add_argument(
        "--datetime",
        type=str,
        help="End datetime in NZDT format: 'YYYY-MM-DD HH:MM'. Defaults to current time.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for reports",
    )
    args = parser.parse_args()
    
    # Determine end time
    if args.datetime:
        end_time_utc = parse_datetime_arg(args.datetime)
        print(f"Using specified time: {args.datetime} NZDT")
    else:
        end_time_utc = datetime.now(timezone.utc)
        print(f"Using current time: {format_nzdt(end_time_utc)} NZDT")
    
    # Output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        paths = PipelinePaths()
        date_str = to_nzdt(end_time_utc).strftime("%Y%m%d")
        output_dir = paths.rain_gauges_dir / date_str / "alarms"
    
    # Setup client
    print("\nConnecting to Moata API...")
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
    )
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        verify_ssl=False,
    )
    client = MoataClient(http=http)
    print("✓ Connected\n")
    
    # Run alarm check
    alarms = check_all_gauges(client, end_time_utc, output_dir)
    
    # Generate reports
    print("\nGenerating reports...")
    generate_html_dashboard(alarms, end_time_utc, output_dir)
    
    # Print formatted alarm list
    print(f"\n{'='*70}")
    print("FORMATTED ALARM LIST")
    print(f"{'='*70}")
    
    if alarms:
        sorted_alarms = sorted(alarms, key=lambda x: x["total_mm"], reverse=True)
        for alarm in sorted_alarms:
            print(
                f"Rainfall Depth: {alarm['gauge_name']} "
                f"{alarm['total_mm']:.0f}mm in the last {alarm['window_description']}, "
                f"{alarm['end_time_nzdt']}."
            )
    else:
        print("No alarms detected.")
    
    print(f"\n{'='*70}")
    print(f"Reports saved to: {output_dir}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
