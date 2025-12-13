from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


class ThresholdChecker:
    """Check time series data for threshold exceedances."""
    
    def check_exceedances(
        self,
        timeseries_df: pd.DataFrame,
        threshold: float,
        trace_name: str = "Max TP108 ARI"
    ) -> List[Dict[str, Any]]:
        """
        Find all timestamps where value exceeded threshold.
        
        Args:
            timeseries_df: DataFrame with 'timestamp' and 'value' columns
            threshold: Threshold value (e.g., 5 year ARI = 5.0)
            trace_name: Trace description for logging
        
        Returns:
            List of exceedance events with timestamp, value, and metadata
        
        Example:
            >>> df = pd.DataFrame({
            ...     'timestamp': ['2024-11-15 05:00:00'],
            ...     'value': [6.8]
            ... })
            >>> checker = ThresholdChecker()
            >>> events = checker.check_exceedances(df, threshold=5.0)
            >>> events[0]['exceeded_by']
            1.8
        """
        # Validate input
        if timeseries_df.empty:
            return []
        
        required_cols = {'timestamp', 'value'}
        if not required_cols.issubset(timeseries_df.columns):
            raise ValueError(f"DataFrame must have columns: {required_cols}")
        
        # Filter to exceedances only
        exceedances = timeseries_df[timeseries_df['value'] > threshold].copy()
        
        if exceedances.empty:
            return []
        
        # Convert to list of event dictionaries
        events = []
        for _, row in exceedances.iterrows():
            events.append({
                'timestamp': row['timestamp'],
                'value': row['value'],
                'threshold': threshold,
                'trace_name': trace_name,
                'exceeded_by': row['value'] - threshold,
                'percent_over': ((row['value'] - threshold) / threshold) * 100
            })
        
        return events
    
    def check_consecutive_exceedances(
        self,
        timeseries_df: pd.DataFrame,
        threshold: float,
        min_consecutive: int = 1,
        max_gap_minutes: int = 15
    ) -> List[Dict[str, Any]]:
        """
        Find groups of consecutive threshold exceedances.
        
        Useful for identifying sustained exceedance periods vs brief spikes.
        
        Args:
            timeseries_df: DataFrame with 'timestamp' and 'value' columns
            threshold: Threshold value
            min_consecutive: Minimum consecutive points to count as event
            max_gap_minutes: Maximum gap between points to still be "consecutive"
        
        Returns:
            List of exceedance events grouped by consecutive periods
        """
        if timeseries_df.empty:
            return []
        
        exceedances = timeseries_df[timeseries_df['value'] > threshold].copy()
        
        if exceedances.empty:
            return []
        
        # Group consecutive exceedances
        exceedances = exceedances.sort_values('timestamp').reset_index(drop=True)
        exceedances['time_diff'] = exceedances['timestamp'].diff().dt.total_seconds() / 60
        
        # Mark new groups where gap exceeds max_gap_minutes
        exceedances['new_group'] = exceedances['time_diff'] > max_gap_minutes
        exceedances['group_id'] = exceedances['new_group'].cumsum()
        
        # Aggregate by group
        groups = []
        for group_id, group_df in exceedances.groupby('group_id'):
            if len(group_df) >= min_consecutive:
                groups.append({
                    'start_time': group_df['timestamp'].min(),
                    'end_time': group_df['timestamp'].max(),
                    'duration_minutes': (
                        (group_df['timestamp'].max() - group_df['timestamp'].min())
                        .total_seconds() / 60
                    ),
                    'num_points': len(group_df),
                    'max_value': group_df['value'].max(),
                    'mean_value': group_df['value'].mean(),
                    'threshold': threshold,
                    'peak_exceeded_by': group_df['value'].max() - threshold
                })
        
        return groups
    
    def summarize_exceedances(
        self,
        events: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Summarize exceedance events for reporting.
        
        Args:
            events: List of exceedance events from check_exceedances()
        
        Returns:
            Summary statistics dictionary
        """
        if not events:
            return {
                'total_exceedances': 0,
                'max_value': None,
                'mean_exceeded_by': None,
                'first_exceedance': None,
                'last_exceedance': None
            }
        
        values = [e['value'] for e in events]
        exceeded_by = [e['exceeded_by'] for e in events]
        timestamps = [e['timestamp'] for e in events]
        
        return {
            'total_exceedances': len(events),
            'max_value': max(values),
            'mean_value': sum(values) / len(values),
            'mean_exceeded_by': sum(exceeded_by) / len(exceeded_by),
            'max_exceeded_by': max(exceeded_by),
            'first_exceedance': min(timestamps),
            'last_exceedance': max(timestamps),
            'threshold': events[0]['threshold']
        }