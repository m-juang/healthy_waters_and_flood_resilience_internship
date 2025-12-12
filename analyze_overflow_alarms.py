"""
test_thresholds_endpoint.py

Test the /v1/traces/{traceId}/thresholds endpoint
to see if this gives us overflow alarm thresholds.

This endpoint might be the KEY to finding threshold configurations!
"""

import os
import json
import time
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

TOKEN_URL = (
    "https://moata.b2clogin.com/"
    "moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token"
)
BASE_API_URL = "https://api.moata.io/ae/v1"
AUCKLAND_RAINFALL_PROJECT_ID = 594
RAIN_GAUGE_ASSET_TYPE_ID = 100

REQUEST_SLEEP = 0.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_client_credentials() -> Dict[str, str]:
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set")
    return {"client_id": client_id, "client_secret": client_secret}

def get_access_token(client_id: str, client_secret: str) -> str:
    logging.info("Requesting access token...")
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    params = {"scope": "https://moata.onmicrosoft.com/moata.io/.default"}
    resp = requests.post(TOKEN_URL, data=data, params=params, timeout=30, verify=False)
    resp.raise_for_status()
    access_token = resp.json().get("access_token")
    if not access_token:
        raise RuntimeError("No access_token in response")
    logging.info("✓ Got access token")
    return access_token

def auth_headers(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

def safe_get(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
    time.sleep(REQUEST_SLEEP)
    logging.debug(f"GET {url}")
    if params:
        logging.debug(f"  Params: {params}")
    
    resp = requests.get(url, headers=headers, params=params, timeout=60, verify=False)
    logging.info(f"  Response: {resp.status_code}")
    
    if resp.status_code in (403, 404):
        logging.warning(f"  ❌ {resp.status_code}")
        return None
    
    resp.raise_for_status()
    return resp.json()

def main():
    logging.info("=" * 80)
    logging.info("Testing /v1/traces/{traceId}/thresholds Endpoint")
    logging.info("=" * 80)
    
    creds = get_client_credentials()
    token = get_access_token(creds["client_id"], creds["client_secret"])
    headers = auth_headers(token)
    
    # Get rain gauges
    logging.info("\n[STEP 1] Fetching rain gauges...")
    url = f"{BASE_API_URL}/projects/{AUCKLAND_RAINFALL_PROJECT_ID}/assets"
    params = {"assetTypeId": RAIN_GAUGE_ASSET_TYPE_ID}
    gauges_data = safe_get(url, headers, params)
    
    if not gauges_data:
        logging.error("❌ Could not fetch rain gauges!")
        return
    
    gauges = gauges_data.get("items", gauges_data) if isinstance(gauges_data, dict) else gauges_data
    logging.info(f"✓ Got {len(gauges)} rain gauges")
    
    # Check first 5 gauges for thresholds
    MAX_GAUGES_TO_CHECK = 5
    thresholds_found = 0
    gauges_with_thresholds = []
    
    logging.info(f"\n[STEP 2] Checking first {MAX_GAUGES_TO_CHECK} gauges for thresholds...")
    
    for gauge_idx, gauge in enumerate(gauges[:MAX_GAUGES_TO_CHECK], 1):
        gauge_name = gauge.get("name", "Unknown")
        asset_id = gauge.get("id") or gauge.get("assetId")
        
        if not asset_id:
            continue
        
        logging.info(f"\n{'='*80}")
        logging.info(f"Gauge {gauge_idx}/{MAX_GAUGES_TO_CHECK}: {gauge_name}")
        logging.info(f"{'='*80}")
        
        # Get traces
        url = f"{BASE_API_URL}/assets/traces"
        params = {"assetId": asset_id}
        traces_data = safe_get(url, headers, params)
        
        if not traces_data:
            logging.info("  No traces found")
            continue
        
        traces = traces_data.get("items", traces_data) if isinstance(traces_data, dict) else traces_data
        logging.info(f"  Found {len(traces)} traces")
        
        gauge_thresholds = []
        
        for trace in traces:
            trace_id = trace.get("id") or trace.get("traceId")
            trace_name = trace.get("name", "Unknown")
            
            if not trace_id:
                continue
            
            # *** NEW ENDPOINT: Get thresholds for this trace ***
            logging.info(f"\n  Checking trace: {trace_name} (ID: {trace_id})")
            url = f"{BASE_API_URL}/traces/{trace_id}/thresholds"
            
            thresholds_data = safe_get(url, headers)
            
            if not thresholds_data:
                logging.info(f"    → No thresholds")
                continue
            
            # Parse thresholds
            thresholds = thresholds_data.get("thresholds", [])
            
            if thresholds:
                thresholds_found += len(thresholds)
                logging.info(f"    ✓ Found {len(thresholds)} threshold(s)!")
                
                for thresh in thresholds:
                    thresh_id = thresh.get("id")
                    thresh_name = thresh.get("name")
                    thresh_value = thresh.get("value")
                    thresh_type = thresh.get("thresholdType")  # Min/Max
                    severity = thresh.get("severity")  # Low/Medium/High/Critical
                    category = thresh.get("category")  # Overflow/etc
                    is_critical = thresh.get("isCritical")
                    alarm_desc = thresh.get("alarmDescription")
                    
                    logging.info(f"\n      Threshold ID: {thresh_id}")
                    logging.info(f"      Name: {thresh_name}")
                    logging.info(f"      Value: {thresh_value}")
                    logging.info(f"      Type: {thresh_type}")
                    logging.info(f"      Severity: {severity}")
                    logging.info(f"      Category: {category}")
                    logging.info(f"      Critical: {is_critical}")
                    logging.info(f"      Description: {alarm_desc}")
                    
                    gauge_thresholds.append({
                        "trace_name": trace_name,
                        "trace_id": trace_id,
                        "threshold": thresh
                    })
            else:
                logging.info(f"    → No thresholds (empty response)")
        
        if gauge_thresholds:
            gauges_with_thresholds.append({
                "gauge_name": gauge_name,
                "gauge_id": asset_id,
                "thresholds": gauge_thresholds
            })
    
    # Summary
    logging.info("\n" + "=" * 80)
    logging.info("SUMMARY")
    logging.info("=" * 80)
    logging.info(f"Gauges checked: {MAX_GAUGES_TO_CHECK}")
    logging.info(f"Gauges with thresholds: {len(gauges_with_thresholds)}")
    logging.info(f"Total thresholds found: {thresholds_found}")
    
    if thresholds_found > 0:
        logging.info("\n✓✓✓ SUCCESS! ✓✓✓")
        logging.info("The /v1/traces/{traceId}/thresholds endpoint WORKS!")
        logging.info("This gives us threshold configurations (values, severity, etc.)")
        
        # Save results
        output_dir = Path("moata_output")
        output_dir.mkdir(exist_ok=True)
        
        output = {
            "summary": {
                "gauges_checked": MAX_GAUGES_TO_CHECK,
                "gauges_with_thresholds": len(gauges_with_thresholds),
                "total_thresholds": thresholds_found,
            },
            "gauges": gauges_with_thresholds
        }
        
        output_path = output_dir / "thresholds_test_results.json"
        output_path.write_text(json.dumps(output, indent=2))
        logging.info(f"\n✓ Results saved to: {output_path}")
        
        logging.info("\n" + "-" * 80)
        logging.info("NEXT STEPS:")
        logging.info("-" * 80)
        logging.info("1. This endpoint gives THRESHOLD CONFIGURATIONS ✓")
        logging.info("2. But does NOT give TRIGGER HISTORY ✗")
        logging.info("3. Need to combine with /alarms/overflow-detailed-info-by-trace")
        logging.info("   to get trigger history (latestStateForThresholdLevel)")
        logging.info("\nRecommendation:")
        logging.info("  Use BOTH endpoints:")
        logging.info("  • /traces/{traceId}/thresholds → Get threshold configs")
        logging.info("  • /alarms/overflow-detailed-info-by-trace → Get trigger history")
        
    else:
        logging.info("\n⚠️  No thresholds found in first 5 gauges")
        logging.info("   Try checking more gauges or different traces")
        logging.info("   Run find_overflow_alarms.py to scan all gauges")
    
    logging.info("=" * 80)

if __name__ == "__main__":
    main()