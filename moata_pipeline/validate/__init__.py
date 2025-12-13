"""
Validation module for verifying alarm triggers against time series data.

This module provides tools to:
- Fetch time series data from Moata API
- Check for threshold exceedances
- Match API events with logged alarms
- Generate validation reports

Example usage:
    from moata_pipeline.validate.runner import run_alarm_validation
    
    results = run_alarm_validation(
        alarm_log_csv=Path("data/alarm_log_ari.csv"),
        output_dir=Path("data/validated_alarms"),
        client=moata_client
    )
"""

from .timeseries_fetcher import TimeSeriesFetcher
from .threshold_checker import ThresholdChecker
from .alarm_matcher import AlarmMatcher

__all__ = [
    "TimeSeriesFetcher",
    "ThresholdChecker",
    "AlarmMatcher",
    "run_alarm_validation",
]