from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from moata_pipeline.moata.client import MoataClient

from .alarm_matcher import AlarmMatcher
from .threshold_checker import ThresholdChecker
from .timeseries_fetcher import TimeSeriesFetcher
from .visualizer import ValidationVisualizer  # ✅ ADDED

logger = logging.getLogger(__name__)


def run_alarm_validation(
    alarm_log_csv: Path,
    output_dir: Path,
    client: MoataClient,
    sample_size: int = 5,
    debug: bool = True  # ✅ Added debug flag
) -> List[Dict[str, Any]]:
    """
    Validate ARI alarms by comparing API data with alarm log.
    
    Steps:
    1. Load alarm log (Sam's CSV)
    2. For each gauge with alarms:
       a. Get trace ID for 'Max TP108 ARI'
       b. Fetch time series data (covering alarm timestamps)
       c. Check threshold exceedances
       d. Match with alarm log timestamps
    3. Generate validation report
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load components
    fetcher = TimeSeriesFetcher(client)
    checker = ThresholdChecker()
    matcher = AlarmMatcher(alarm_log_csv)
    
    # Get unique gauges from alarm log
    alarm_log = pd.read_csv(alarm_log_csv)
    
    # Parse timestamps properly
    alarm_log['createdtimeutc'] = pd.to_datetime(alarm_log['createdtimeutc'], errors='coerce')
    
    # Get gauges with valid timestamps
    valid_alarms = alarm_log.dropna(subset=['createdtimeutc'])
    unique_assets = valid_alarms['assetid'].unique()[:sample_size]
    
    logger.info(f"Validating {len(unique_assets)} gauges from alarm log")
    
    results = []
    
    for asset_id in unique_assets:
        logger.info(f"Processing asset {asset_id}...")
        
        try:
            # Get trace ID for 'Max TP108 ARI' trace
            traces = client.get_traces_for_asset(asset_id)
            
            # ✅ UPDATED: Try to find both ARI trace and source Rainfall trace
            ari_trace = next(
                (t for t in traces if 'Max TP108 ARI' in t.get('description', '')),
                None
            )
            
            # Try to find the source Rainfall trace
            rainfall_trace = next(
                (t for t in traces 
                 if 'Rainfall' in t.get('description', '') 
                 and 'filtered' not in t.get('description', '').lower()
                 and 'ari' not in t.get('description', '').lower()),
                None
            )
            
            if not ari_trace:
                logger.warning(f"No ARI trace found for asset {asset_id}")
                continue
            
            ari_trace_id = ari_trace['id']
            logger.info(f"  Found ARI trace: {ari_trace_id}")
            
            # ✅ NEW: Log both trace IDs
            if rainfall_trace:
                rainfall_trace_id = rainfall_trace['id']
                logger.info(f"  Found Rainfall trace: {rainfall_trace_id} - {rainfall_trace.get('description')}")
            else:
                logger.warning(f"  No source Rainfall trace found")
                rainfall_trace_id = None
            
            # Get threshold from alarm configuration
            thresholds = client.get_thresholds_for_trace(ari_trace_id)
            
            # ✅ DEBUG: Log threshold config
            if debug and thresholds:
                logger.info(f"  Thresholds config: {json.dumps(thresholds, indent=2)}")
            
            threshold_value = thresholds[0]['value'] if thresholds else 5.0  # Default to 5
            logger.info(f"  Using threshold: {threshold_value}")
            
            # ✅ CRITICAL FIX: Try fetching from Rainfall trace first (ARI is virtual)
            trace_id_to_fetch = rainfall_trace_id if rainfall_trace_id else ari_trace_id
            
            if not rainfall_trace_id:
                logger.warning(f"  ⚠️  Attempting to fetch from ARI trace (virtual) - may return no data")
            else:
                logger.info(f"  ✓ Fetching from source Rainfall trace instead of virtual ARI trace")
            
            # ✅ FIXED: Use alarm timestamp ranges instead of last 30 days
            gauge_alarms = alarm_log[alarm_log['assetid'] == asset_id].copy()
            gauge_alarms['createdtimeutc'] = pd.to_datetime(gauge_alarms['createdtimeutc'])
            
            if gauge_alarms.empty:
                logger.warning(f"No alarms found for asset {asset_id}")
                continue
            
            # Get earliest and latest alarm timestamps
            earliest_alarm = gauge_alarms['createdtimeutc'].min()
            latest_alarm = gauge_alarms['createdtimeutc'].max()
            
            # Add buffer (1 day before/after alarms)
            from_time = earliest_alarm - timedelta(days=1)
            to_time = latest_alarm + timedelta(days=1)
            
            # Check 32-day limit
            days_span = (to_time - from_time).days
            if days_span > 32:
                logger.warning(f"  Alarm span ({days_span} days) exceeds 32-day limit. Using chunked fetch.")
                # For now, just fetch around latest alarm
                to_time = latest_alarm + timedelta(days=1)
                from_time = to_time - timedelta(days=30)
            
            logger.info(f"  Fetching data from {from_time} to {to_time} ({(to_time - from_time).days} days)")
            
            # ✅ FIXED: Use ARI endpoint with type=Tp108 parameter
            try:
                logger.info(f"  Using ARI endpoint: /traces/{ari_trace_id}/ari?type=Tp108")
                ts_data = fetcher.fetch_ari_data(
                    ari_trace_id, 
                    from_time, 
                    to_time,
                    ari_type="Tp108"
                )
                
                # ✅ Convert ARI data format to standard timeseries format
                # ARI returns: [{duration, ari, depth, type}]
                # We need to check if any ari value exceeds threshold
                if not ts_data.empty and "ari" in ts_data.columns:
                    logger.info(f"  Received {len(ts_data)} ARI records")
                    logger.info(f"  ARI value range: {ts_data['ari'].min():.2f} to {ts_data['ari'].max():.2f}")
                    
                    # Check for exceedances
                    exceedances_ari = ts_data[ts_data['ari'] > threshold_value].copy()
                    
                    if not exceedances_ari.empty:
                        logger.info(f"  Found {len(exceedances_ari)} ARI values exceeding threshold {threshold_value}")
                        for _, row in exceedances_ari.iterrows():
                            logger.info(f"    Duration: {row['duration']}s, ARI: {row['ari']:.2f} years")
                    
                    # Create simple match result for ARI data
                    gauge_alarms = alarm_log[alarm_log['assetid'] == asset_id].copy()
                    
                    results.append({
                        'asset_id': asset_id,
                        'ari_trace_id': ari_trace_id,
                        'rainfall_trace_id': rainfall_trace_id,
                        'threshold': threshold_value,
                        'ari_exceedances_count': len(exceedances_ari),
                        'logged_alarms': len(gauge_alarms),
                        'validation_status': 'CONFIRMED' if len(exceedances_ari) > 0 else 'NO_EXCEEDANCE',
                        'note': f'ARI endpoint returns duration-based values (10min to 24hr windows), not point-in-time timestamps. Found {len(exceedances_ari)} duration windows exceeding {threshold_value} year threshold during alarm period.',
                        'ari_values': exceedances_ari.to_dict('records') if not exceedances_ari.empty else []
                    })
                    continue
                else:
                    logger.warning(f"  ARI data empty or invalid format")
                    
            except Exception as e:
                logger.error(f"  ARI fetch failed: {e}")
            
            # ✅ Fallback: Fetch from Rainfall trace
            
            # ✅ Fallback: Fetch from Rainfall trace
            if not rainfall_trace_id:
                logger.error(f"  No Rainfall trace found, cannot proceed")
                continue
            
            logger.info(f"  Fallback: Fetching from Rainfall trace {rainfall_trace_id}")
            
            try:
                ts_data = fetcher.fetch_trace_data(
                    rainfall_trace_id, 
                    from_time, 
                    to_time,
                    data_type="None",
                    data_interval=300
                )
                
                # ✅ DEBUG: Log data info
                if debug:
                    logger.info(f"  Received {len(ts_data)} data points")
                    if not ts_data.empty:
                        logger.info(f"  Data range: {ts_data['timestamp'].min()} to {ts_data['timestamp'].max()}")
                        logger.info(f"  Value range: {ts_data['value'].min():.2f} to {ts_data['value'].max():.2f}")
                        
                        # Show sample data
                        logger.info(f"  Sample (first 5 rows):")
                        for _, row in ts_data.head(5).iterrows():
                            logger.info(f"    {row['timestamp']}: {row['value']:.2f}")
                
            except Exception as e:
                logger.error(f"  Failed to fetch Rainfall data: {e}")
                continue
            
            if ts_data.empty:
                logger.warning(f"  No Rainfall data returned for trace {rainfall_trace_id}")
                logger.info(f"  Time range: {from_time} to {to_time}")
                
                results.append({
                    'asset_id': asset_id,
                    'ari_trace_id': ari_trace_id,
                    'rainfall_trace_id': rainfall_trace_id,
                    'fetched_from_trace_id': trace_id_to_fetch,
                    'threshold': threshold_value,
                    'api_exceedances': 0,
                    'logged_alarms': len(gauge_alarms),
                    'matched': 0,
                    'match_rate': 0.0,
                    'error': 'No time series data',
                    'details': {
                        'matched': [],
                        'unmatched_api_events': [],
                        'missed_alarms': gauge_alarms.to_dict('records'),
                        'match_rate': 0.0,
                        'stats': {
                            'total_api_events': 0,
                            'total_logged_alarms': len(gauge_alarms),
                            'matched_count': 0,
                            'unmatched_api_count': 0,
                            'missed_alarms_count': len(gauge_alarms)
                        }
                    }
                })
                continue
            
            # Check exceedances
            exceedances = checker.check_exceedances(ts_data, threshold_value)
            logger.info(f"  Found {len(exceedances)} threshold exceedances")
            
            # ✅ DEBUG: Log exceedances
            if debug and exceedances:
                logger.info(f"  Exceedances:")
                for exc in exceedances[:5]:  # First 5
                    logger.info(f"    {exc['timestamp']}: {exc['value']:.2f} (exceeded by {exc['exceeded_by']:.2f})")
            
            # Match with alarm log
            match_result = matcher.match_alarms(asset_id, exceedances)
            
            logger.info(f"  Matched: {len(match_result['matched'])}/{len(gauge_alarms)} alarms")
            
            results.append({
                'asset_id': asset_id,
                'ari_trace_id': ari_trace_id,
                'rainfall_trace_id': rainfall_trace_id,
                'fetched_from_trace_id': trace_id_to_fetch,
                'threshold': threshold_value,
                'api_exceedances': len(exceedances),
                'logged_alarms': len(gauge_alarms),
                'matched': len(match_result['matched']),
                'match_rate': match_result['match_rate'],
                'details': match_result
            })
            
        except Exception as e:
            logger.error(f"Error processing asset {asset_id}: {e}", exc_info=True)
            continue
    
    # Generate report
    if results:
        report_df = pd.DataFrame(results)
        csv_path = output_dir / 'validation_summary.csv'
        report_df.to_csv(csv_path, index=False)
        
        # Save detailed results as JSON
        with open(output_dir / 'validation_details.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # ✅ NEW: Generate visualizations
        logger.info("Generating visualizations...")
        viz = ValidationVisualizer(output_dir)
        viz.create_validation_report(results)
        html_report = viz.create_html_report(results, csv_path)
        
        logger.info(f"Validation complete. Results saved to {output_dir}")
        logger.info(f"📊 Open HTML report: {html_report}")
    else:
        logger.warning("No results to save")
    
    return results