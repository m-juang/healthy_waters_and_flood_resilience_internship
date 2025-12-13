from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from moata_pipeline.moata.client import MoataClient
from .timeseries_fetcher import TimeSeriesFetcher
from .threshold_checker import ThresholdChecker
from .alarm_matcher import AlarmMatcher

logger = logging.getLogger(__name__)


def run_alarm_validation(
    alarm_log_csv: Path,
    output_dir: Path,
    client: MoataClient,
    sample_size: int = 5  # Start with 5 gauges for testing
) -> None:
    """
    Validate ARI alarms by comparing API data with alarm log.
    
    Steps:
    1. Load alarm log (Sam's CSV)
    2. For each gauge with alarms:
       a. Get trace ID for 'Max TP108 ARI'
       b. Fetch time series data (last 30 days)
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
    unique_assets = alarm_log['assetid'].unique()[:sample_size]
    
    logger.info(f"Validating {len(unique_assets)} gauges from alarm log")
    
    results = []
    
    for asset_id in unique_assets:
        logger.info(f"Processing asset {asset_id}...")
        
        try:
            # Get trace ID for 'Max TP108 ARI' trace
            traces = client.get_traces_for_asset(asset_id)
            ari_trace = next(
                (t for t in traces if 'Max TP108 ARI' in t.get('description', '')),
                None
            )
            
            if not ari_trace:
                logger.warning(f"No ARI trace found for asset {asset_id}")
                continue
            
            trace_id = ari_trace['id']
            
            # Get threshold from alarm configuration
            thresholds = client.get_thresholds_for_trace(trace_id)
            threshold_value = thresholds[0]['value'] if thresholds else None
            
            if not threshold_value:
                logger.warning(f"No threshold found for trace {trace_id}")
                continue
            
            # Fetch time series (last 30 days to stay under 32-day limit)
            to_time = datetime.now()
            from_time = to_time - timedelta(days=30)
            
            ts_data = fetcher.fetch_trace_data(trace_id, from_time, to_time)
            
            # Check exceedances
            exceedances = checker.check_exceedances(ts_data, threshold_value)
            
            # Match with alarm log
            match_result = matcher.match_alarms(asset_id, exceedances)
            
            results.append({
                'asset_id': asset_id,
                'trace_id': trace_id,
                'threshold': threshold_value,
                'api_exceedances': len(exceedances),
                'logged_alarms': len(matcher.alarm_log[matcher.alarm_log['assetid'] == asset_id]),
                'matched': len(match_result['matched']),
                'match_rate': match_result['match_rate'],
                'details': match_result
            })
            
        except Exception as e:
            logger.error(f"Error processing asset {asset_id}: {e}")
            continue
    
    # Generate report
    report_df = pd.DataFrame(results)
    report_df.to_csv(output_dir / 'validation_summary.csv', index=False)
    
    logger.info(f"Validation complete. Results saved to {output_dir}")
    
    return results