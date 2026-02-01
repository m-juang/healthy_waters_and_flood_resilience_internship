"""
Check Alarms Script

Fetches all rain gauge alarms in the last 24 hours and verifies rainfall data.
Outputs formatted alarm list with actual rainfall values.

OPTIMIZED (v2.0): Now uses cached gauge data from retrieve step to avoid 
duplicate API calls. Only fetches realtime rainfall data as needed.

Usage:
    python check_alarms.py --datetime "2026-01-21 22:40"
    python check_alarms.py  # Uses current time
    python check_alarms.py --date 2026-01-21  # Uses cached data from specific date

Output Format:
    Rainfall Depth: ACC - Rain - Swanson @ Waitakere 100mm in the last 24 hours, 2026-01-21 22:20:00

Author: Auckland Council Internship Team
Version: 2.0.0 - Optimized to reuse cached gauge data from retrieve step
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


# =============================================================================
# CACHED DATA LOADER (NEW - Avoids duplicate API calls)
# =============================================================================

def load_cached_gauge_data(date_str: Optional[str] = None) -> Optional[List[Dict]]:
    """
    Load gauge data from cached retrieve output.
    
    This avoids duplicate API calls by reusing data already collected
    by the retrieve step.
    
    Args:
        date_str: Date string YYYY-MM-DD to find cached data. 
                  If None, auto-detects most recent available.
    
    Returns:
        List of gauge dictionaries with traces and thresholds, or None if not found.
    """
    import json
    
    # Build possible cache paths to check
    cache_paths = []
    
    if date_str:
        # Use specified date
        paths = PipelinePaths.for_date(date_str)
        cache_paths.append(paths.rain_gauges_traces_alarms_json)
    else:
        # Auto-detect: look for most recent date range folder
        gauges_base = Path("outputs/rain_gauges")
        if gauges_base.exists():
            # Find all date range folders sorted by name (most recent first)
            date_folders = sorted(
                [d for d in gauges_base.iterdir() if d.is_dir() and "-" in d.name],
                key=lambda x: x.name,
                reverse=True
            )
            for folder in date_folders:
                cache_file = folder / "raw" / "rain_gauges_traces_alarms.json"
                if cache_file.exists():
                    cache_paths.append(cache_file)
                    break
    
    # Try to load from cache
    for cache_path in cache_paths:
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"✓ Using cached gauge data from: {cache_path}")
                print(f"  ({len(data)} gauges loaded from cache)")
                return data
            except (json.JSONDecodeError, IOError) as e:
                print(f"[!] Failed to load cache {cache_path}: {e}")
                continue
    
    return None


def extract_gauge_info_from_cache(cached_data: List[Dict]) -> Tuple[List[Dict], Dict[int, List[Dict]]]:
    """
    Extract gauge list and traces from cached data.
    
    Args:
        cached_data: Raw data from rain_gauges_traces_alarms.json
        
    Returns:
        Tuple of (gauge_list, traces_by_asset_id)
    """
    gauges = []
    traces_by_asset = {}
    
    for item in cached_data:
        gauge = item.get("gauge", {})
        traces = item.get("traces", [])
        
        gauge_id = gauge.get("id")
        if gauge_id:
            gauges.append(gauge)
            traces_by_asset[gauge_id] = traces
    
    return gauges, traces_by_asset


def extract_thresholds_from_traces(traces: List[Dict]) -> Dict[float, float]:
    """
    Extract alarm thresholds from trace data.
    
    Looks for threshold definitions in the trace data and maps
    them to time windows (hours -> mm threshold).
    
    Args:
        traces: List of trace dictionaries
        
    Returns:
        Dict mapping window_hours -> threshold_mm
    """
    # Default thresholds (fallback if not found in trace data)
    defaults = {
        0.5: 10,   # 10mm in 30 minutes
        1: 15,     # 15mm in 60 minutes
        6: 50,     # 50mm in 6 hours
        12: 70,    # 70mm in 12 hours
        24: 100,   # 100mm in 24 hours
    }
    
    # Try to extract from trace thresholds
    thresholds = {}
    for trace in traces:
        desc = trace.get("description", "").lower()
        for threshold_info in trace.get("thresholds", []):
            threshold_value = threshold_info.get("value", 0)
            
            # Parse window from description
            if "30 min" in desc or "30min" in desc:
                thresholds[0.5] = threshold_value
            elif "60 min" in desc or "1 hour" in desc or "60min" in desc:
                thresholds[1] = threshold_value
            elif "6 hour" in desc or "6h" in desc:
                thresholds[6] = threshold_value
            elif "12 hour" in desc or "12h" in desc:
                thresholds[12] = threshold_value
            elif "24 hour" in desc or "24h" in desc:
                thresholds[24] = threshold_value
    
    # Fill in any missing with defaults
    for hours, default_val in defaults.items():
        if hours not in thresholds:
            thresholds[hours] = default_val
    
    return thresholds


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
    output_dir: Path,
    cached_data: Optional[List[Dict]] = None,
) -> List[Dict]:
    """
    Check all rain gauges for alarms in the last 24 hours.
    
    OPTIMIZED: Uses cached gauge data if available to avoid duplicate API calls.
    Only fetches realtime rainfall data.
    
    Args:
        client: MoataClient instance for API calls
        end_time_utc: End time for alarm check
        output_dir: Directory for output files
        cached_data: Optional pre-loaded gauge data from retrieve step
    
    Returns:
        List of alarm records
    """
    print(f"\n{'='*70}")
    print("RAIN GAUGE ALARM CHECK")
    print(f"{'='*70}")
    print(f"End Time (UTC): {end_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time (NZDT): {format_nzdt(end_time_utc)}")
    print(f"Lookback: {LOOKBACK_HOURS} hours")
    print(f"{'='*70}\n")
    
    # Use cached data if available (avoids duplicate API calls!)
    if cached_data:
        print("📦 Using CACHED gauge data (no duplicate API calls)")
        gauges, traces_by_asset = extract_gauge_info_from_cache(cached_data)
        print(f"✓ Found {len(gauges)} rain gauges from cache\n")
    else:
        # Fallback: fetch from API (slower, duplicate call)
        print("⚠️  No cached data - fetching from API (slower)...")
        print("   TIP: Run 'gauge-retrieve --date YYYY-MM-DD' first to cache data")
        gauges = client.get_rain_gauges(project_id=DEFAULT_PROJECT_ID, asset_type_id=100)
        print(f"✓ Found {len(gauges)} rain gauges from API\n")
        
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
        
        # Use thresholds from cached data if available, otherwise use defaults
        if cached_data:
            thresholds = extract_thresholds_from_traces(traces)
        else:
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
    
    # Calculate period start (24 hours before end time)
    start_time_utc = end_time_utc - timedelta(hours=LOOKBACK_HOURS)
    
    # Format times for display
    period_start_utc = start_time_utc.strftime("%Y-%m-%d %H:%M:%S")
    period_end_utc = end_time_utc.strftime("%Y-%m-%d %H:%M:%S")
    period_start_nzdt = format_nzdt(start_time_utc)
    period_end_nzdt = format_nzdt(end_time_utc)
    
    # Sort alarms by rainfall amount
    sorted_alarms = sorted(alarms, key=lambda x: x["total_mm"], reverse=True) if alarms else []
    
    # Generate alarm rows (without Time column)
    alarm_rows = ""
    for alarm in sorted_alarms:
        severity_class = "critical" if alarm["total_mm"] >= 100 else "warning" if alarm["total_mm"] >= 50 else "normal"
        alarm_rows += f"""
        <tr class="{severity_class}">
            <td>{alarm['gauge_name']}</td>
            <td class="rainfall">{alarm['total_mm']:.1f} mm</td>
            <td>{alarm['window_description']}</td>
            <td>{alarm['threshold_mm']} mm</td>
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
        .period-info {{
            margin-top: 15px;
            padding: 15px 20px;
            background: rgba(0, 212, 255, 0.1);
            border-radius: 8px;
            display: inline-block;
            text-align: left;
        }}
        .period-info .label {{
            color: #00d4ff;
            font-weight: 600;
            margin-bottom: 10px;
            display: block;
            text-align: center;
        }}
        .period-row {{
            display: flex;
            gap: 10px;
            margin: 5px 0;
            align-items: center;
        }}
        .period-row .tz-label {{
            color: #888;
            font-size: 0.85rem;
            width: 50px;
            text-align: right;
        }}
        .period-row .dates {{
            color: #fff;
            font-size: 0.95rem;
        }}
        .arrow {{
            color: #00d4ff;
            margin: 0 5px;
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
                Generated: {format_nzdt(datetime.now(timezone.utc))} NZDT
            </p>
            <div class="period-info">
                <div class="label">📅 Analysis Period ({LOOKBACK_HOURS} hours)</div>
                <div class="period-row">
                    <span class="tz-label">NZDT:</span>
                    <span class="dates">{period_start_nzdt} <span class="arrow">→</span> {period_end_nzdt}</span>
                </div>
                <div class="period-row">
                    <span class="tz-label">UTC:</span>
                    <span class="dates">{period_start_utc} <span class="arrow">→</span> {period_end_utc}</span>
                </div>
            </div>
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
            {"<table><thead><tr><th>Gauge Name</th><th>Rainfall</th><th>Period</th><th>Threshold</th></tr></thead><tbody>" + alarm_rows + "</tbody></table>" if alarms else "<div class='no-alarms'>✅ No alarms in the last " + str(LOOKBACK_HOURS) + " hours</div>"}
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
    parser = argparse.ArgumentParser(
        description="Check rain gauge alarms (OPTIMIZED - uses cached data)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                   # Check last 24h from NOW (end=now)
  %(prog)s --date 2026-01-21                 # Check 24h starting from 2026-01-21 00:00 UTC
  %(prog)s --datetime "2026-01-21 22:40"     # Check 24h ending at specific time (NZDT input)
  %(prog)s --no-cache                        # Force fresh API calls (slower)

Time Period Behavior:
  --date YYYY-MM-DD      : START = date 00:00:00 UTC, check forward 24h (consistent with retrieve.py)
  --datetime "..." NZDT  : END = specified time, check backward 24h  
  (no argument)          : END = current time, check backward 24h

Timezone:
  --date uses UTC (same as retrieve.py for consistency)
  --datetime input is NZDT (local time), converted to UTC internally

Performance:
  With cached data: ~1-2 minutes (only fetches rainfall values)
  Without cache:    ~5-10 minutes (fetches gauge list + traces + rainfall)

Note:
  Run 'gauge-retrieve --date YYYY-MM-DD' first to cache gauge data.
        """
    )
    parser.add_argument(
        "--datetime",
        type=str,
        help="END datetime in NZDT format: 'YYYY-MM-DD HH:MM'. Checks 24h backward from this time.",
    )
    parser.add_argument(
        "--date",
        type=str,
        metavar="YYYY-MM-DD",
        help="START date in UTC. Checks 24h forward from 00:00:00 UTC. Consistent with retrieve.py.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Force fresh API calls, don't use cached data.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for reports",
    )
    args = parser.parse_args()
    
    # Determine time period based on arguments
    if args.datetime:
        # Explicit datetime provided - use as end time (input is NZDT, convert to UTC)
        end_time_utc = parse_datetime_arg(args.datetime)
        print(f"Using specified time: {args.datetime} NZDT")
        print(f"  Checking period: {format_nzdt(end_time_utc - timedelta(hours=LOOKBACK_HOURS))} to {format_nzdt(end_time_utc)} NZDT")
    elif args.date:
        # Date provided - use START of that day in UTC (consistent with retrieve.py)
        # Example: --date 2026-01-21 → 2026-01-21 00:00:00 UTC to 2026-01-22 00:00:00 UTC
        from datetime import datetime as dt
        try:
            date_obj = dt.strptime(args.date, "%Y-%m-%d")
            # Start of day in UTC = 00:00:00 UTC (consistent with gauge/retrieve.py)
            start_time_utc = date_obj.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
            # End time = start + 24 hours
            end_time_utc = start_time_utc + timedelta(hours=LOOKBACK_HOURS)
            print(f"Using date {args.date} as START of period (UTC)")
            print(f"  UTC:  {start_time_utc.strftime('%Y-%m-%d %H:%M:%S')} to {end_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  NZDT: {format_nzdt(start_time_utc)} to {format_nzdt(end_time_utc)}")
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # No date/datetime - use current time as END, go backward 24 hours
        end_time_utc = datetime.now(timezone.utc)
        print(f"Using current time as END: {format_nzdt(end_time_utc)} NZDT")
        print(f"  Checking period: {format_nzdt(end_time_utc - timedelta(hours=LOOKBACK_HOURS))} to {format_nzdt(end_time_utc)} NZDT")
    
    # Load cached data (unless --no-cache is specified)
    cached_data = None
    if not args.no_cache:
        cached_data = load_cached_gauge_data(args.date)
        if cached_data is None:
            print("\n⚠️  No cached gauge data found.")
            print("   For faster execution, run retrieve first:")
            print("     gauge-retrieve --date YYYY-MM-DD")
            print("   Continuing with API calls...\n")
    
    # Output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Calculate actual analysis period (24 hours before end_time)
        start_time_utc = end_time_utc - timedelta(hours=LOOKBACK_HOURS)
        
        # Convert to NZDT for folder naming (to match local date)
        start_nzdt = to_nzdt(start_time_utc)
        end_nzdt = to_nzdt(end_time_utc)
        
        # Use PipelinePaths with actual date range
        paths = PipelinePaths.for_date_range(start_nzdt, end_nzdt)
        output_dir = paths.rain_gauges_dir / "alarms"
    
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
    
    # Run alarm check (with cached data if available)
    alarms = check_all_gauges(client, end_time_utc, output_dir, cached_data)
    
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
