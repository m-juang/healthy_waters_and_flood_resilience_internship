"""
moata_rain_alerts.py

Script to:
1. Obtain an access token for the Moata API using client credentials.
2. Fetch rain gauge assets for the Auckland Rainfall project (ID = 594).
3. For each rain gauge, fetch its traces.
4. For each trace, fetch configured overflow alarms.
5. Save everything into JSON files for later analysis.

Before running:
- Install dependencies: pip install requests python-dotenv
- Set environment variables:
    MOATA_CLIENT_ID
    MOATA_CLIENT_SECRET
or use the hardcoded credentials below (already set from Sam's email).

Note from Sam:
- Do NOT send requests asynchronously to avoid impacting system performance
- API is rate limited to 800 requests in 5 minutes
- Tokens are valid for 1 hour
- There are 2 kinds of alarms:
  * 'recency' alarms: triggered if data hasn't been updated for X amount of time
  * 'overflow' alarms: triggered when trace value exceeds a certain value
- Main trace for rain gauges is called 'Rainfall' (has recency alarm setup)
- Some old/inactive gauges may not have newer trace/alarm setups
"""

import os
import time
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

import requests
# Disable SSL warnings when verify=False is used
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables from a .env file (if present)
from dotenv import load_dotenv
# Load from explicit path in case working directory is different
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# =========================
# CONFIGURATION
# =========================

# Token endpoint from Sam's email
TOKEN_URL = (
    "https://moata.b2clogin.com/"
    "moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token"
)

# Base API URL - updated to match Swagger documentation
BASE_API_URL = "https://api.moata.io/ae/v1"

# Project & filters
AUCKLAND_RAINFALL_PROJECT_ID = 594  # from Sam
RAIN_GAUGE_ASSET_TYPE_ID = 100      # rain gauge assetTypeId

# Rate limiting safety (Sam: 800 requests / 5 minutes)
# Using 2 requests/second = 600 requests/5 minutes to stay safe
REQUESTS_PER_SECOND = 2.0
REQUEST_SLEEP = 1.0 / REQUESTS_PER_SECOND

# OAuth token lifetime handling
# Tokens issued by the Moata OAuth server are valid for ~1 hour.
# Refresh the token if it's within this buffer of expiry to avoid 401s
TOKEN_TTL_SECONDS = 3600
TOKEN_REFRESH_BUFFER_SECONDS = 300  # refresh 5 minutes before expiry

# Global hook for safe_get to refresh tokens when a 401 is encountered.
# In `main()` we set `REFRESH_TOKEN` to a function that returns a fresh token
# and updates `TOKEN_ACQUIRED_AT` so the main loop knows when token was refreshed.
REFRESH_TOKEN: Optional[Callable[[], str]] = None
TOKEN_ACQUIRED_AT: Optional[float] = None
# Output directory
OUTPUT_DIR = Path("moata_output")


# =========================
# LOGGING SETUP
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# =========================
# AUTHENTICATION
# =========================

def get_client_credentials() -> Dict[str, str]:
    """
    Read `MOATA_CLIENT_ID` and `MOATA_CLIENT_SECRET` from environment variables.

    This function expects either:
      - environment variables to be set in the environment, or
      - a local `.env` file containing the keys (see `.env.example`).

    For security, do NOT commit your actual `.env` file to GitHub. The
    repository contains `.env.example` with placeholders and `.gitignore`
    excludes `.env`.
    """
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set as environment "
            "variables or placed in a local .env file. See .env.example for the "
            "expected names."
        )

    return {"client_id": client_id, "client_secret": client_secret}


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Obtain an access token using client credentials flow.
    Token is valid for 1 hour (per Sam's email).
    
    Uses POST request with form data as specified by Sam.
    """
    logging.info("Requesting access token...")

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    params = {
        "scope": "https://moata.onmicrosoft.com/moata.io/.default"
    }

    # Use POST as per OAuth2 standard
    # Note: verify=False is used due to SSL certificate issues with Moata API
    resp = requests.post(TOKEN_URL, data=data, params=params, timeout=30, verify=False)
    resp.raise_for_status()
    token_data = resp.json()

    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError(f"No access_token in response: {token_data}")

    logging.info("Successfully obtained access token (valid for 1 hour).")
    return access_token


def auth_headers(access_token: str) -> Dict[str, str]:
    """Return headers with bearer token for API authentication."""
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


# =========================
# API HELPER
# =========================

def safe_get(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    sleep: float = REQUEST_SLEEP,
    allow_404: bool = False,
) -> Any:
    """
    Wrapper around requests.get with basic error handling and rate limiting.
    
    Note from Sam: Do NOT send requests asynchronously to avoid impacting
    system performance. This function enforces sequential requests with delays.
    
    Note: verify=False is used due to SSL certificate issues with Moata API
    
    Args:
        allow_404: If True, return None on 404 instead of raising error
    """
    time.sleep(sleep)  # Rate limiting: stay under 800 requests / 5 minutes
    logging.debug(f"GET {url} params={params}")

    # Perform request
    resp = requests.get(url, headers=headers, params=params, timeout=60, verify=False)

    # Handle 404 gracefully if allowed (e.g., trace has no alarms)
    if resp.status_code == 404 and allow_404:
        logging.debug(f"404 for {url} - resource not found (this is OK)")
        return None

    # Handle 403 Forbidden gracefully if allowed (e.g., endpoint not accessible)
    if resp.status_code == 403 and allow_404:
        logging.debug(f"403 Forbidden for {url} - endpoint not accessible (this is OK)")
        return None

    # If unauthorized and we have a refresh hook, try to refresh token and retry once
    if resp.status_code == 401 and REFRESH_TOKEN is not None:
        logging.warning("401 Unauthorized for %s - attempting token refresh and retry", url)
        try:
            new_token = REFRESH_TOKEN()
            # update headers for retry
            headers = dict(headers)
            headers["Authorization"] = f"Bearer {new_token}"
            # short sleep before retry to avoid immediate rejections
            time.sleep(1)
            resp = requests.get(url, headers=headers, params=params, timeout=60, verify=False)
        except Exception as e:
            logging.warning("Token refresh failed: %s", e)

    # After potential retry, handle 404/403 again
    if resp.status_code == 404 and allow_404:
        logging.debug(f"404 for {url} - resource not found (this is OK)")
        return None
    if resp.status_code == 403 and allow_404:
        logging.debug(f"403 Forbidden for {url} - endpoint not accessible (this is OK)")
        return None

    resp.raise_for_status()
    return resp.json()


# =========================
# DOMAIN-SPECIFIC CALLS
# =========================

def get_rain_gauges(
    access_token: str,
    project_id: int = AUCKLAND_RAINFALL_PROJECT_ID,
    asset_type_id: int = RAIN_GAUGE_ASSET_TYPE_ID,
) -> List[Dict[str, Any]]:
    """
    Fetch rain gauge 'assets' for the given project.
    
    Uses endpoint from Sam's workflow:
        GET /projects/{projectId}/assets?assetTypeId=100
    
    assetTypeId=100 limits results to rain gauges only.
    Note: This returns ALL rain gauges including old/inactive ones.
    """
    url = f"{BASE_API_URL}/projects/{project_id}/assets"
    headers = auth_headers(access_token)
    params = {"assetTypeId": asset_type_id}

    logging.info(
        "Fetching rain gauge assets for project_id=%s (assetTypeId=%s)...",
        project_id, asset_type_id
    )
    data = safe_get(url, headers=headers, params=params)

    # Handle both list and dict responses
    if isinstance(data, dict) and "items" in data:
        gauges = data["items"]
    else:
        gauges = data if isinstance(data, list) else []

    logging.info("Fetched %d rain gauges (includes active and inactive).", len(gauges))
    return gauges


def get_traces_for_asset(
    access_token: str,
    asset_id: Any,
) -> List[Dict[str, Any]]:
    """
    Fetch traces (time series) for a given asset (rain gauge).
    
    Uses endpoint from Sam's workflow:
        GET /assets/traces?assetId={assetId}
    
    Note from Sam:
    - Rain gauges have multiple traces for different purposes
    - Main trace is called 'Rainfall' (shows recorded values, has recency alarm)
    - Other traces are setup to trigger overflow alarms at certain values
    - Inactive gauges may not have newer trace/alarm setups
    """
    url = f"{BASE_API_URL}/assets/traces"
    headers = auth_headers(access_token)
    params = {"assetId": asset_id}

    logging.debug("Fetching traces for asset_id=%s...", asset_id)
    data = safe_get(url, headers=headers, params=params)

    if isinstance(data, dict) and "items" in data:
        traces = data["items"]
    else:
        traces = data if isinstance(data, list) else []

    logging.info("Asset %s has %d traces.", asset_id, len(traces))
    return traces


def get_alarms_for_trace(
    access_token: str,
    trace_id: Any,
) -> List[Dict[str, Any]]:
    """
    Fetch overflow alarms configured for a given trace.
    
    Uses endpoint from Swagger documentation:
        GET /v1/alarms/overflow-detailed-info-by-trace?traceId={traceId}
    
    Note from Sam:
    - This returns 'overflow' alarms (triggered when trace value exceeds a threshold)
    - There are also 'recency' alarms (triggered if data hasn't been updated)
    - The main 'Rainfall' trace has recency alarms that send messages to council
    - Not all traces have alarms configured (will return 404)
    """
    url = f"{BASE_API_URL}/alarms/overflow-detailed-info-by-trace"
    headers = auth_headers(access_token)
    params = {"traceId": trace_id}

    logging.debug("Fetching overflow alarms for trace_id=%s...", trace_id)
    data = safe_get(url, headers=headers, params=params, allow_404=True)
    
    # If 404 (no alarms for this trace), return empty list
    if data is None:
        logging.debug("Trace %s has no overflow alarms configured.", trace_id)
        return []

    if isinstance(data, dict) and "items" in data:
        alarms = data["items"]
    else:
        alarms = data if isinstance(data, list) else []

    if alarms:
        logging.info("Trace %s has %d overflow alarm(s).", trace_id, len(alarms))
    
    return alarms


def get_detailed_alarms_by_project(
    access_token: str,
    project_id: int = AUCKLAND_RAINFALL_PROJECT_ID,
) -> Dict[int, Dict[str, Any]]:
    """
    Fetch ALL detailed alarm information for a project (both overflow and recency alarms).
    
    Uses endpoint from Swagger spec:
        GET /v1/alarms/detailed-alarms?projectId={projectId}
    
    Returns a dictionary mapping traceId -> alarm details (including thresholds, severity, etc.)
    
    Note: This endpoint returns both:
    - 'OverflowMonitoring' alarms (threshold-based)
    - 'DataRecency' alarms (staleness-based) with configuration like maxLookbackOverride
    
    This provides the missing threshold/severity/enabled data for recency alarms.
    
    If the endpoint is not available (403 Forbidden), returns empty dict and continues.
    """
    url = f"{BASE_API_URL}/alarms/detailed-alarms"
    headers = auth_headers(access_token)
    params = {"projectId": project_id}

    logging.info("Fetching detailed alarms for project_id=%s (all types)...", project_id)
    try:
        data = safe_get(url, headers=headers, params=params, allow_404=True)
    except Exception as e:
        # If endpoint not available (403, etc.), log and continue
        logging.warning(
            f"Could not fetch detailed alarms (endpoint may not be available): {e}. "
            f"Will continue with overflow alarms only."
        )
        return {}
    
    if data is None:
        logging.info("No alarms found for project %s.", project_id)
        return {}

    # Parse response - should be a list of alarm objects with traceId field
    alarms_list = data if isinstance(data, list) else data.get("items", [])
    
    # Build dictionary indexed by traceId for easy lookup
    alarms_by_trace: Dict[int, Dict[str, Any]] = {}
    for alarm in alarms_list:
        trace_id = alarm.get("traceId")
        if trace_id is not None:
            alarms_by_trace[trace_id] = alarm
            alarm_type = alarm.get("alarmType", "Unknown")
            logging.debug(f"  Alarm for trace {trace_id}: type={alarm_type}")
    
    logging.info("Fetched %d detailed alarms for project.", len(alarms_by_trace))
    return alarms_by_trace


# =========================
# ORCHESTRATION / MAIN
# =========================

def main():
    """
    Main workflow as described by Sam:
    1. Get rain gauge assets from project 594
    2. For each rain gauge, get its traces
    3. For each trace, get configured overflow alarms
    4. Fetch detailed alarm info (including recency alarms with thresholds)
    5. Save all data for analysis
    
    Note: Sequential processing only (no async) per Sam's request.
    """
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Get credentials & token
    logging.info("=" * 60)
    logging.info("Starting Moata API data collection")
    logging.info("=" * 60)
    
    creds = get_client_credentials()
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]

    access_token = get_access_token(client_id, client_secret)
    # Track when token was acquired so we can refresh during long runs
    token_acquired_at = time.time()
    token_ttl = TOKEN_TTL_SECONDS
    token_refresh_buffer = TOKEN_REFRESH_BUFFER_SECONDS

    # Expose a module-level refresh callback so safe_get can refresh on 401s
    def _refresh_and_record_token() -> str:
        nonlocal access_token, token_acquired_at
        new_token = get_access_token(client_id, client_secret)
        access_token = new_token
        token_acquired_at = time.time()
        # Also update module-level marker so other parts can observe it
        global TOKEN_ACQUIRED_AT
        TOKEN_ACQUIRED_AT = token_acquired_at
        return new_token

    # Set global REFRESH_TOKEN hook
    global REFRESH_TOKEN
    REFRESH_TOKEN = _refresh_and_record_token
    # Record initial acquisition time at module-level too
    global TOKEN_ACQUIRED_AT
    TOKEN_ACQUIRED_AT = token_acquired_at

    # 2. Fetch rain gauges
    logging.info("\nStep 1: Fetching rain gauge assets...")
    rain_gauges = get_rain_gauges(access_token)
    gauges_path = OUTPUT_DIR / "rain_gauges.json"
    rain_gauges_json = json.dumps(rain_gauges, indent=2)
    gauges_path.write_text(rain_gauges_json)
    logging.info("✓ Saved %d rain gauges to %s", len(rain_gauges), gauges_path)
    
    # Print entire rain gauges JSON to terminal
    logging.info("\n" + "=" * 60)
    logging.info("FULL JSON: Rain Gauges")
    logging.info("=" * 60)
    print(rain_gauges_json)
    logging.info("=" * 60 + "\n")

    # 2b. Fetch detailed alarms for the project (both overflow and recency)
    logging.info("\nStep 1b: Fetching detailed alarms (overflow + recency)...")
    detailed_alarms = get_detailed_alarms_by_project(access_token)
    logging.info("✓ Fetched %d detailed alarms", len(detailed_alarms))
    
    # Show preview of first few alarms
    if detailed_alarms:
        logging.info("\n--- PREVIEW: First 3 Detailed Alarms ---")
        for i, (trace_id, alarm) in enumerate(list(detailed_alarms.items())[:3]):
            print(f"\nTrace ID {trace_id}:")
            print(json.dumps(alarm, indent=2))
        logging.info("--- END PREVIEW ---\n")

    # 3. For each gauge, fetch traces and alarms
    logging.info("\nStep 2: Fetching traces and alarms for each gauge...")
    logging.info("(This may take a while - processing sequentially per Sam's request)")
    
    all_data: List[Dict[str, Any]] = []
    total_gauges = len(rain_gauges)

    for idx, gauge in enumerate(rain_gauges, start=1):
        asset_id = gauge.get("id") or gauge.get("assetId")
        gauge_name = gauge.get("name", "Unknown")
        
        if asset_id is None:
            logging.warning("Gauge without id encountered: %s", gauge)
            continue

        logging.info(
            "\nProcessing gauge %d/%d: '%s' (asset_id=%s)...", 
            idx, total_gauges, gauge_name, asset_id
        )

        # Refresh access token if it's close to expiry to avoid 401 Unauthorized
        elapsed = time.time() - token_acquired_at
        if elapsed >= (token_ttl - token_refresh_buffer):
            logging.info("Access token nearing expiry (%.0fs elapsed) - refreshing...", elapsed)
            try:
                access_token = get_access_token(client_id, client_secret)
                token_acquired_at = time.time()
                logging.info("Access token refreshed successfully.")
            except Exception as e:
                logging.warning("Failed to refresh access token: %s. Continuing with existing token.", e)

        # Get traces for this gauge
        traces = get_traces_for_asset(access_token, asset_id)

        # For each trace, get its alarms
        traces_with_alarms = []
        for trace in traces:
            trace_id = trace.get("id") or trace.get("traceId")
            trace_name = trace.get("name", "Unknown")
            
            if trace_id is None:
                logging.warning("Trace without id encountered: %s", trace)
                continue

            # Highlight the main 'Rainfall' trace
            if trace_name == "Rainfall":
                logging.info("  → Found main 'Rainfall' trace (id=%s)", trace_id)

            alarms = get_alarms_for_trace(access_token, trace_id)
            
            # Add detailed alarm info if available for this trace
            detailed_alarm = detailed_alarms.get(trace_id)
            
            # Log if alarms found for this trace
            if alarms or detailed_alarm:
                logging.info(f"    Trace '{trace_name}' (id={trace_id}):")
                if alarms:
                    logging.info(f"      - Found {len(alarms)} overflow alarm(s)")
                if detailed_alarm:
                    alarm_type = detailed_alarm.get("alarmType", "Unknown")
                    logging.info(f"      - Has detailed alarm: type={alarm_type}")
            
            trace_entry = {
                "trace": trace,
                "overflow_alarms": alarms,  # Renamed for clarity
                "detailed_alarm": detailed_alarm,  # NEW: includes recency alarm details
            }
            traces_with_alarms.append(trace_entry)

        asset_entry = {
            "gauge": gauge,
            "traces": traces_with_alarms,
        }
        all_data.append(asset_entry)
        
        # Print this gauge's data immediately (real-time output)
        logging.info("\n" + "-" * 60)
        logging.info(f"GAUGE {idx}/{total_gauges} DATA: {gauge_name}")
        logging.info("-" * 60)
        print(json.dumps(asset_entry, indent=2))
        logging.info("-" * 60 + "\n")

    # 4. Save combined structure
    logging.info("\n" + "=" * 60)
    all_path = OUTPUT_DIR / "rain_gauges_traces_alarms.json"
    all_data_json = json.dumps(all_data, indent=2)
    all_path.write_text(all_data_json)
    logging.info("✓ Saved complete data to %s", all_path)
    
    # Print entire combined JSON to terminal
    logging.info("\n" + "=" * 60)
    logging.info("FULL JSON: Rain Gauges + Traces + Alarms")
    logging.info("=" * 60)
    print(all_data_json)
    logging.info("=" * 60 + "\n")
    
    # Summary
    total_traces = sum(len(g["traces"]) for g in all_data)
    total_alarms = sum(
        len(t["overflow_alarms"]) 
        for g in all_data 
        for t in g["traces"]
    )
    total_detailed = sum(
        1 for g in all_data 
        for t in g["traces"] 
        if t.get("detailed_alarm") is not None
    )
    
    logging.info("\n" + "=" * 60)
    logging.info("Summary:")
    logging.info("  Rain gauges: %d", len(all_data))
    logging.info("  Total traces: %d", total_traces)
    logging.info("  Total overflow alarms: %d", total_alarms)
    logging.info("  Total traces with detailed alarms: %d", total_detailed)
    logging.info("=" * 60)
    logging.info("Done! Check the '%s' directory for output files.", OUTPUT_DIR)


if __name__ == "__main__":
    main()