"""
filter_active_rain_gauges.py

Script to filter and analyze rain gauge data collected by moata_rain_alerts.py
Based on Sam's email guidance:
1. Filter out Northland gauges (keep only Auckland)
2. Filter out inactive gauges (check telemeteredMaximumTime on 'Rainfall' trace)
3. Use alternative endpoint to get primary 'Rainfall' traces efficiently
4. Generate summary of active gauges with their alarm configurations

Requirements:
- Run moata_rain_alerts.py first to generate input files
- Input files expected:
    * moata_output/rain_gauges_traces_alarms.json
- Install: pip install pandas python-dateutil

Output:
- Filtered list of active Auckland rain gauges
- Summary of traces with alarms
- Alarm threshold configurations
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import pandas as pd
    from dateutil import parser as date_parser
except ImportError:
    print("ERROR: Missing dependencies. Please install:")
    print("  pip install pandas python-dateutil")
    exit(1)

# =========================
# CONFIGURATION
# =========================

BASE_API_URL = "https://api.moata.io/ae/v1"
AUCKLAND_RAINFALL_PROJECT_ID = 594
RAINFALL_DATA_VARIABLE_TYPE_ID = 10  # From Sam's email
RAINFALL_DESCRIPTION = "Rainfall"    # From Sam's email

# Inactive threshold: gauges with no data in last 3 months
INACTIVE_THRESHOLD_MONTHS = 3

# Input/output directories
INPUT_DIR = Path("moata_output")
OUTPUT_DIR = Path("moata_filtered")

# Rate limiting
REQUEST_SLEEP = 0.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# =========================
# HELPER FUNCTIONS
# =========================

def load_json_file(filepath: Path) -> Any:
    """Load and parse JSON file."""
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    with open(filepath, 'r') as f:
        return json.load(f)


def is_auckland_gauge(gauge_name: str) -> bool:
    """
    Check if gauge is an Auckland gauge (not Northland).
    Northland gauges have "Northland" in their name.
    """
    return "northland" not in gauge_name.lower()


def parse_telemetered_time(time_str: Optional[str]) -> Optional[datetime]:
    """Parse telemeteredMaximumTime string to datetime object."""
    if not time_str:
        return None
    try:
        return date_parser.parse(time_str)
    except Exception as e:
        logging.warning(f"Could not parse time '{time_str}': {e}")
        return None


def is_gauge_active(telemetered_time: Optional[datetime], 
                    months_threshold: int = INACTIVE_THRESHOLD_MONTHS) -> bool:
    """
    Check if gauge is active based on telemeteredMaximumTime.
    Active = data received within last X months.
    """
    if not telemetered_time:
        return False
    
    cutoff_date = datetime.now(telemetered_time.tzinfo) - timedelta(days=30 * months_threshold)
    return telemetered_time >= cutoff_date


def get_rainfall_trace(traces_data: List[Dict]) -> Optional[Dict]:
    """
    Extract the primary 'Rainfall' trace from a gauge's traces.
    This is the main record for each asset per Sam's email.
    
    Looking for trace where description contains 'Rainfall' or 
    dataVariableType.name is 'Rainfall' or similar.
    """
    for trace_data in traces_data:
        trace = trace_data.get("trace", {})
        description = trace.get("description", "")
        
        # Check if this is the main Rainfall trace
        # Based on the data structure, it seems the description field is what we need
        if description == "Rainfall":
            return trace_data
        
        # Also check dataVariableType
        data_var_type = trace.get("dataVariableType", {})
        if data_var_type.get("name") == "Rainfall" or data_var_type.get("type") == "Rain":
            # Additional check: prefer traces with "Rainfall" in description
            if "rainfall" in description.lower() and "filtered" not in description.lower():
                return trace_data
    
    # If no exact match, look for any rainfall-related trace
    for trace_data in traces_data:
        trace = trace_data.get("trace", {})
        description = trace.get("description", "")
        if "rainfall" in description.lower() and "mirror" not in description.lower():
            return trace_data
    
    return None


# =========================
# API CALLS
# =========================

def get_fresh_token() -> str:
    """Get a fresh access token using client credentials."""
    import os
    
    TOKEN_URL = (
        "https://moata.b2clogin.com/"
        "moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token"
    )
    
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise RuntimeError(
            "MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set as environment "
            "variables or placed in a local .env file."
        )
    
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    params = {
        "scope": "https://moata.onmicrosoft.com/moata.io/.default"
    }
    
    resp = requests.post(TOKEN_URL, data=data, params=params, timeout=30, verify=False)
    resp.raise_for_status()
    token_data = resp.json()
    
    return token_data.get("access_token")


def get_rainfall_traces_info(access_token: str, 
                             project_id: int = AUCKLAND_RAINFALL_PROJECT_ID) -> List[Dict]:
    """
    Use Sam's suggested endpoint to get all 'Rainfall' traces efficiently.
    
    Endpoint: GET /v1/projects/{projectId}/traces_info
    Params: dataVariableTypeId=10, description=Rainfall
    
    This is more efficient than fetching traces for each asset individually.
    """
    url = f"{BASE_API_URL}/projects/{project_id}/traces_info"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    params = {
        "dataVariableTypeId": RAINFALL_DATA_VARIABLE_TYPE_ID,
        "description": RAINFALL_DESCRIPTION,
    }
    
    logging.info("Fetching all 'Rainfall' traces using efficient endpoint...")
    
    import time
    time.sleep(REQUEST_SLEEP)
    
    resp = requests.get(url, headers=headers, params=params, timeout=60, verify=False)
    resp.raise_for_status()
    data = resp.json()
    
    if isinstance(data, dict) and "items" in data:
        traces = data["items"]
    else:
        traces = data if isinstance(data, list) else []
    
    logging.info(f"Found {len(traces)} 'Rainfall' traces")
    return traces


# =========================
# ANALYSIS FUNCTIONS
# =========================

def filter_existing_data(all_data: List[Dict]) -> Dict[str, Any]:
    """
    Filter the data collected by moata_rain_alerts.py
    
    Returns dict with:
    - active_gauges: List of active Auckland gauges
    - inactive_gauges: List of inactive gauges (for reference)
    - northland_gauges: List of Northland gauges (excluded)
    - stats: Summary statistics
    """
    active_gauges = []
    inactive_gauges = []
    northland_gauges = []
    no_rainfall_trace = []
    
    logging.info(f"\nProcessing {len(all_data)} gauges...")
    
    for gauge_data in all_data:
        gauge = gauge_data.get("gauge", {})
        gauge_name = gauge.get("name", "Unknown")
        gauge_id = gauge.get("id")
        
        # Filter out Northland gauges
        if not is_auckland_gauge(gauge_name):
            northland_gauges.append(gauge_data)
            logging.debug(f"Excluding Northland gauge: {gauge_name}")
            continue
        
        # Get the primary 'Rainfall' trace
        rainfall_trace_data = get_rainfall_trace(gauge_data.get("traces", []))
        
        if not rainfall_trace_data:
            logging.warning(f"No 'Rainfall' trace found for gauge: {gauge_name} (id={gauge_id})")
            no_rainfall_trace.append(gauge_data)
            continue
        
        # Check telemeteredMaximumTime
        rainfall_trace = rainfall_trace_data.get("trace", {})
        telemetered_time_str = rainfall_trace.get("telemeteredMaximumTime")
        
        if not telemetered_time_str:
            logging.warning(
                f"No telemeteredMaximumTime for gauge: {gauge_name} (id={gauge_id}) - "
                f"trace description: {rainfall_trace.get('description')}"
            )
            inactive_gauges.append(gauge_data)
            continue
        
        telemetered_time = parse_telemetered_time(telemetered_time_str)
        
        if not telemetered_time:
            logging.warning(f"Could not parse time for gauge: {gauge_name}")
            inactive_gauges.append(gauge_data)
            continue
        
        # Check if active (data within last 3 months)
        if is_gauge_active(telemetered_time):
            gauge_data["last_data_time"] = telemetered_time.isoformat()
            gauge_data["last_data_time_dt"] = telemetered_time
            gauge_data["rainfall_trace"] = rainfall_trace  # Store for easy access
            active_gauges.append(gauge_data)
            logging.info(
                f"✓ Active: {gauge_name[:60]} (last: {telemetered_time.strftime('%Y-%m-%d')})"
            )
        else:
            inactive_gauges.append(gauge_data)
            logging.debug(
                f"Inactive: {gauge_name} (last data: {telemetered_time.strftime('%Y-%m-%d')})"
            )
    
    stats = {
        "total_gauges": len(all_data),
        "active_auckland_gauges": len(active_gauges),
        "inactive_gauges": len(inactive_gauges),
        "northland_gauges": len(northland_gauges),
        "no_rainfall_trace": len(no_rainfall_trace),
    }
    
    logging.info(f"\nFiltering complete:")
    logging.info(f"  Total: {stats['total_gauges']}")
    logging.info(f"  Active Auckland: {stats['active_auckland_gauges']}")
    logging.info(f"  Inactive: {stats['inactive_gauges']}")
    logging.info(f"  Northland: {stats['northland_gauges']}")
    logging.info(f"  No Rainfall trace: {stats['no_rainfall_trace']}")
    
    return {
        "active_gauges": active_gauges,
        "inactive_gauges": inactive_gauges,
        "northland_gauges": northland_gauges,
        "no_rainfall_trace": no_rainfall_trace,
        "stats": stats,
    }


def analyze_alarms(active_gauges: List[Dict]) -> pd.DataFrame:
    """
    Analyze alarm configurations for active gauges.
    Create a summary table showing:
    - Gauge name
    - Traces with alarms
    - Alarm thresholds (from both overflow and detailed/recency alarms)
    
    Now extracts threshold/severity/enabled from:
    - overflow_alarms: Traditional overflow/threshold alarms
    - detailed_alarm: Comprehensive alarm data including recency alarms with thresholds
    """
    records = []
    
    for gauge_data in active_gauges:
        gauge = gauge_data.get("gauge", {})
        gauge_name = gauge.get("name", "Unknown")
        gauge_id = gauge.get("id")
        last_data = gauge_data.get("last_data_time_dt")
        
        traces = gauge_data.get("traces", [])
        
        for trace_data in traces:
            trace = trace_data.get("trace", {})
            trace_name = trace.get("description", "Unknown")
            trace_id = trace.get("id")
            has_alarms = trace.get("hasAlarms", False)
            
            # Get overflow alarms (traditional method)
            alarms = trace_data.get("overflow_alarms", [])
            
            # Get detailed alarm info (includes both overflow and recency alarms)
            detailed_alarm = trace_data.get("detailed_alarm")
            
            # Extract threshold info from detailed_alarm if available
            detailed_threshold = None
            detailed_severity = None
            detailed_enabled = None
            alarm_type = None
            
            if detailed_alarm:
                alarm_type = detailed_alarm.get("alarmType", "Unknown")
                
                # Get alarm thresholds array
                alarm_thresholds = detailed_alarm.get("alarmThresholds", [])
                if alarm_thresholds:
                    # Use first threshold
                    first_threshold = alarm_thresholds[0]
                    detailed_threshold = first_threshold.get("thresholdValue")
                    detailed_severity = first_threshold.get("alarmSeverity")
                
                # For recency alarms, get configuration from other fields
                if alarm_type == "DataRecency":
                    # Recency alarms use maxLookbackOverride for staleness threshold
                    detailed_threshold = detailed_alarm.get("maxLookbackOverride", detailed_threshold)
                    detailed_enabled = detailed_alarm.get("alarmState") == "Enabled"
            
            # Process overflow alarms
            if alarms:
                for alarm in alarms:
                    records.append({
                        "gauge_id": gauge_id,
                        "gauge_name": gauge_name,
                        "last_data": last_data.strftime('%Y-%m-%d') if last_data else None,
                        "trace_id": trace_id,
                        "trace_name": trace_name,
                        "alarm_id": alarm.get("id"),
                        "alarm_name": alarm.get("name", ""),
                        "alarm_type": "OverflowMonitoring",
                        "threshold": alarm.get("threshold", detailed_threshold),
                        "severity": alarm.get("severity", detailed_severity),
                        "enabled": alarm.get("enabled", detailed_enabled if detailed_enabled is not None else True),
                    })
            elif has_alarms:
                # Trace has alarms flag (might be recency alarms)
                # Use detailed_alarm data if available
                if detailed_alarm:
                    records.append({
                        "gauge_id": gauge_id,
                        "gauge_name": gauge_name,
                        "last_data": last_data.strftime('%Y-%m-%d') if last_data else None,
                        "trace_id": trace_id,
                        "trace_name": trace_name,
                        "alarm_id": detailed_alarm.get("alarmId"),
                        "alarm_name": detailed_alarm.get("description", f"{alarm_type} Alarm"),
                        "alarm_type": alarm_type,
                        "threshold": detailed_threshold,
                        "severity": detailed_severity,
                        "enabled": detailed_enabled,
                    })
                else:
                    # Has alarms flag but no detailed data (recency alarms)
                    records.append({
                        "gauge_id": gauge_id,
                        "gauge_name": gauge_name,
                        "last_data": last_data.strftime('%Y-%m-%d') if last_data else None,
                        "trace_id": trace_id,
                        "trace_name": trace_name,
                        "alarm_id": None,
                        "alarm_name": "Has alarms (recency)",
                        "alarm_type": "DataRecency",
                        "threshold": None,
                        "severity": None,
                        "enabled": None,
                    })
    
    df = pd.DataFrame(records)
    return df


def create_summary_report(filtered_data: Dict, alarms_df: pd.DataFrame) -> str:
    """Create a text summary report."""
    stats = filtered_data["stats"]
    active_gauges = filtered_data["active_gauges"]
    
    report_lines = [
        "=" * 80,
        "AUCKLAND RAIN GAUGE ANALYSIS REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        "",
        "FILTERING RESULTS:",
        f"  Total gauges in dataset: {stats['total_gauges']}",
        f"  ✓ Active Auckland gauges: {stats['active_auckland_gauges']}",
        f"  ✗ Inactive gauges (>3 months): {stats['inactive_gauges']}",
        f"  ✗ Northland gauges: {stats['northland_gauges']}",
        f"  ✗ No Rainfall trace: {stats['no_rainfall_trace']}",
        "",
        "=" * 80,
        "ACTIVE GAUGE DETAILS:",
        "=" * 80,
        "",
    ]
    
    # Sort by last data time (most recent first)
    sorted_gauges = sorted(
        active_gauges, 
        key=lambda g: g.get("last_data_time_dt", datetime.min.replace(tzinfo=None)),
        reverse=True
    )
    
    for gauge_data in sorted_gauges:
        gauge = gauge_data.get("gauge", {})
        gauge_name = gauge.get("name", "Unknown")
        gauge_id = gauge.get("id")
        last_data = gauge_data.get("last_data_time_dt")
        
        traces = gauge_data.get("traces", [])
        traces_with_alarms = sum(
            1 for t in traces if t.get("overflow_alarms") or t.get("trace", {}).get("hasAlarms")
        )
        total_alarms = sum(
            len(t.get("overflow_alarms", [])) for t in traces
        )
        
        report_lines.append(f"• {gauge_name}")
        report_lines.append(f"  ID: {gauge_id}")
        report_lines.append(f"  Last data: {last_data.strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"  Traces: {len(traces)} total, {traces_with_alarms} with alarms configured")
        report_lines.append(f"  Overflow alarms: {total_alarms}")
        report_lines.append("")
    
    report_lines.extend([
        "=" * 80,
        "ALARM CONFIGURATION SUMMARY:",
        "=" * 80,
        "",
    ])
    
    if not alarms_df.empty:
        # Count alarms by trace type
        alarms_only = alarms_df[alarms_df["alarm_id"].notna()]
        if not alarms_only.empty:
            report_lines.append(f"Total overflow alarms configured: {len(alarms_only)}")
            report_lines.append("")
            
            by_trace = alarms_only.groupby("trace_name").size().sort_values(ascending=False)
            report_lines.append("Alarms by trace type:")
            for trace_name, count in by_trace.items():
                report_lines.append(f"  {trace_name}: {count} alarms")
            report_lines.append("")
            
            # Show threshold ranges
            if "threshold" in alarms_only.columns:
                report_lines.append("Threshold ranges by trace type:")
                for trace_name in alarms_only["trace_name"].unique():
                    trace_alarms = alarms_only[alarms_only["trace_name"] == trace_name]
                    thresholds = trace_alarms["threshold"].dropna()
                    if not thresholds.empty:
                        report_lines.append(
                            f"  {trace_name}: "
                            f"{thresholds.min():.1f} - {thresholds.max():.1f} mm"
                        )
        
        # Count traces with hasAlarms flag but no overflow alarms
        has_alarms_flag = alarms_df[
            (alarms_df["alarm_id"].isna()) & 
            (alarms_df["alarm_name"] == "Has alarms (not overflow)")
        ]
        if not has_alarms_flag.empty:
            report_lines.append("")
            report_lines.append(
                f"Traces with alarms (likely recency alarms): "
                f"{len(has_alarms_flag)}"
            )
    else:
        report_lines.append("No overflow alarms found on active gauges.")
    
    report_lines.append("")
    report_lines.append("=" * 80)
    
    return "\n".join(report_lines)


# =========================
# MAIN
# =========================

def main():
    """
    Main workflow:
    1. Load data from moata_rain_alerts.py output
    2. Filter out Northland and inactive gauges
    3. Analyze alarm configurations
    4. Generate summary reports
    """
    logging.info("=" * 80)
    logging.info("Starting rain gauge data filtering and analysis")
    logging.info("=" * 80)
    
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # 1. Load existing data
    logging.info("\nStep 1: Loading data from previous collection...")
    
    try:
        all_data_path = INPUT_DIR / "rain_gauges_traces_alarms.json"
        all_data = load_json_file(all_data_path)
        logging.info(f"✓ Loaded {len(all_data)} gauges from {all_data_path}")
    except FileNotFoundError:
        logging.error(f"Input file not found: {all_data_path}")
        logging.error("Please run moata_rain_alerts.py first to collect data.")
        return
    
    # 2. Filter data
    logging.info("\nStep 2: Filtering data...")
    logging.info(f"  Criteria:")
    logging.info(f"    - Must be Auckland gauge (not Northland)")
    logging.info(f"    - Must have Rainfall trace")
    logging.info(f"    - Must have data within last {INACTIVE_THRESHOLD_MONTHS} months")
    
    filtered_data = filter_existing_data(all_data)
    
    # Save filtered data
    active_path = OUTPUT_DIR / "active_auckland_gauges.json"
    with open(active_path, 'w') as f:
        # Convert datetime objects to strings for JSON serialization
        active_copy = []
        for g in filtered_data["active_gauges"]:
            g_copy = g.copy()
            if "last_data_time_dt" in g_copy:
                del g_copy["last_data_time_dt"]
            active_copy.append(g_copy)
        json.dump(active_copy, f, indent=2)
    
    logging.info(f"\n✓ Saved {len(filtered_data['active_gauges'])} active gauges to {active_path}")
    
    # 3. Analyze alarms
    logging.info("\nStep 3: Analyzing alarm configurations...")
    
    alarms_df = analyze_alarms(filtered_data["active_gauges"])
    
    # Save alarm summary
    if not alarms_df.empty:
        alarms_csv_path = OUTPUT_DIR / "alarm_summary.csv"
        alarms_df.to_csv(alarms_csv_path, index=False)
        logging.info(f"✓ Saved alarm summary to {alarms_csv_path}")
        
        # Also save as JSON for easier inspection
        alarms_json_path = OUTPUT_DIR / "alarm_summary.json"
        alarms_df.to_json(alarms_json_path, orient="records", indent=2)
        logging.info(f"✓ Saved alarm summary to {alarms_json_path}")
    else:
        logging.warning("No alarm data to save")
    
    # 4. Generate summary report
    logging.info("\nStep 4: Generating summary report...")
    
    report = create_summary_report(filtered_data, alarms_df)
    
    report_path = OUTPUT_DIR / "analysis_report.txt"
    with open(report_path, 'w') as f:
        f.write(report)
    logging.info(f"✓ Saved report to {report_path}")
    
    # Print report to console
    print("\n" + report)
    
    # 5. Optional: Use Sam's suggested endpoint for comparison
    logging.info("\nStep 5 (Optional): Fetching Rainfall traces using efficient endpoint...")
    logging.info("This validates our filtering against the API's direct query...")
    
    try:
        access_token = get_fresh_token()
        rainfall_traces = get_rainfall_traces_info(access_token)
        
        # Create DataFrame for comparison
        if rainfall_traces:
            traces_df = pd.DataFrame(rainfall_traces)
            
            if not traces_df.empty:
                # Get active asset IDs from our filtered data
                active_asset_ids = set(
                    g["gauge"].get("id") 
                    for g in filtered_data["active_gauges"]
                )
                
                # Check which rainfall traces belong to our active gauges
                if "assetId" in traces_df.columns:
                    traces_df["in_active_set"] = traces_df["assetId"].isin(active_asset_ids)
                
                # Parse telemetered times
                if "telemeteredMaximumTime" in traces_df.columns:
                    traces_df["telemetered_dt"] = traces_df["telemeteredMaximumTime"].apply(
                        parse_telemetered_time
                    )
                    traces_df["is_active"] = traces_df["telemetered_dt"].apply(
                        lambda x: is_gauge_active(x) if x else False
                    )
                
                validation_path = OUTPUT_DIR / "rainfall_traces_validation.csv"
                traces_df.to_csv(validation_path, index=False)
                logging.info(f"✓ Saved validation data to {validation_path}")
                
                # Quick stats
                if "is_active" in traces_df.columns:
                    api_active = traces_df["is_active"].sum()
                    our_active = len(active_asset_ids)
                    logging.info(f"\nValidation results:")
                    logging.info(f"  API shows {api_active} active Rainfall traces")
                    logging.info(f"  Our filtering found {our_active} active gauges")
        else:
            logging.info("No rainfall traces returned from API")
            
    except Exception as e:
        logging.warning(f"Could not fetch validation data from API: {e}")
        logging.warning("Skipping validation step (main analysis is complete)")
    
    # Final summary
    logging.info("\n" + "=" * 80)
    logging.info("COMPLETE!")
    logging.info("=" * 80)
    logging.info(f"Output files saved to: {OUTPUT_DIR}/")
    logging.info("  - active_auckland_gauges.json (filtered gauge data)")
    logging.info("  - alarm_summary.csv (alarm configurations)")
    logging.info("  - alarm_summary.json (alarm configurations)")
    logging.info("  - analysis_report.txt (summary report)")
    logging.info("  - rainfall_traces_validation.csv (API validation, if successful)")
    logging.info("=" * 80)


if __name__ == "__main__":
    main()