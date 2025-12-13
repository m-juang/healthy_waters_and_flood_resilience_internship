from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


class AlarmMatcher:
    """Match API exceedance events with logged alarms."""
    
    def __init__(self, alarm_log_path: Path | str) -> None:
        """
        Load Sam's alarm log CSV.
        
        Args:
            alarm_log_path: Path to alarm_log_ari.csv
        
        Expected CSV columns:
            - assetid: Rain gauge asset ID
            - name: Gauge name
            - description: Trace description (e.g., "Max TP108 ARI")
            - alertid: Unique alarm ID
            - createdtimeutc: Alarm timestamp
        """
        alarm_log_path = Path(alarm_log_path)
        
        if not alarm_log_path.exists():
            raise FileNotFoundError(f"Alarm log not found: {alarm_log_path}")
        
        self.alarm_log = pd.read_csv(alarm_log_path)
        
        # Parse timestamp column
        # Handle both full timestamp and time-only format from Sam's CSV
        self.alarm_log['createdtimeutc'] = pd.to_datetime(
            self.alarm_log['createdtimeutc'],
            errors='coerce'
        )
        
        # Remove rows with invalid timestamps
        invalid_count = self.alarm_log['createdtimeutc'].isna().sum()
        if invalid_count > 0:
            print(f"Warning: {invalid_count} rows with invalid timestamps removed")
            self.alarm_log = self.alarm_log.dropna(subset=['createdtimeutc'])
    
    def match_alarms(
        self,
        asset_id: int,
        exceedance_events: List[Dict[str, Any]],
        tolerance_minutes: int = 15
    ) -> Dict[str, Any]:
        """
        Match API exceedances with alarm log entries.
        
        Args:
            asset_id: Rain gauge asset ID
            exceedance_events: List of threshold exceedances from API
            tolerance_minutes: Time tolerance for matching (default 15 min)
        
        Returns:
            Dictionary with:
                - matched: List of matched event pairs
                - unmatched_api_events: API events without matching alarm
                - missed_alarms: Logged alarms without matching API event
                - match_rate: Fraction of logged alarms that matched API
        
        Example:
            >>> matcher = AlarmMatcher("data/alarm_log_ari.csv")
            >>> events = [{'timestamp': datetime(2024,11,15,5,0), 'value': 6.8}]
            >>> result = matcher.match_alarms(3160974, events, tolerance_minutes=15)
            >>> result['match_rate']
            1.0
        """
        # Filter alarm log to this gauge
        gauge_alarms = self.alarm_log[
            self.alarm_log['assetid'] == asset_id
        ].copy()
        
        if gauge_alarms.empty:
            return {
                'matched': [],
                'unmatched_api_events': exceedance_events,
                'missed_alarms': [],
                'match_rate': 0.0,
                'note': f'No alarms in log for asset {asset_id}'
            }
        
        matched = []
        unmatched_api = []
        matched_alarm_ids = set()
        
        # For each API exceedance, try to find matching alarm
        for event in exceedance_events:
            event_time = pd.to_datetime(event['timestamp'])
            
            # Calculate time difference to all alarms for this gauge
            time_diffs = (gauge_alarms['createdtimeutc'] - event_time).abs()
            
            # Find alarms within tolerance window
            within_tolerance = time_diffs <= timedelta(minutes=tolerance_minutes)
            
            if within_tolerance.any():
                # Get closest alarm
                closest_idx = time_diffs[within_tolerance].idxmin()
                closest_alarm = gauge_alarms.loc[closest_idx]
                
                matched.append({
                    'api_timestamp': event_time,
                    'api_value': event['value'],
                    'alarm_timestamp': closest_alarm['createdtimeutc'],
                    'alert_id': closest_alarm['alertid'],
                    'time_diff_seconds': time_diffs.loc[closest_idx].total_seconds(),
                    'gauge_name': closest_alarm['name'],
                    'exceeded_by': event.get('exceeded_by', None)
                })
                
                matched_alarm_ids.add(closest_alarm['alertid'])
            else:
                unmatched_api.append(event)
        
        # Find logged alarms with no matching API event
        missed_alarms = gauge_alarms[
            ~gauge_alarms['alertid'].isin(matched_alarm_ids)
        ].to_dict('records')
        
        # Calculate match rate based on logged alarms
        total_logged = len(gauge_alarms)
        match_rate = len(matched) / total_logged if total_logged > 0 else 0.0
        
        return {
            'matched': matched,
            'unmatched_api_events': unmatched_api,
            'missed_alarms': missed_alarms,
            'match_rate': match_rate,
            'stats': {
                'total_api_events': len(exceedance_events),
                'total_logged_alarms': total_logged,
                'matched_count': len(matched),
                'unmatched_api_count': len(unmatched_api),
                'missed_alarms_count': len(missed_alarms)
            }
        }
    
    def get_alarm_summary(self) -> pd.DataFrame:
        """
        Get summary of alarm log by gauge.
        
        Returns:
            DataFrame with alarm counts per gauge
        """
        summary = self.alarm_log.groupby(['assetid', 'name']).agg(
            alarm_count=('alertid', 'count'),
            first_alarm=('createdtimeutc', 'min'),
            last_alarm=('createdtimeutc', 'max')
        ).reset_index()
        
        return summary.sort_values('alarm_count', ascending=False)
    
    def get_alarms_for_asset(self, asset_id: int) -> pd.DataFrame:
        """
        Get all logged alarms for a specific asset.
        
        Args:
            asset_id: Rain gauge asset ID
        
        Returns:
            DataFrame of alarms for this asset
        """
        return self.alarm_log[
            self.alarm_log['assetid'] == asset_id
        ].sort_values('createdtimeutc')