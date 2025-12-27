"""
Analysis Reporting Module

Generates text-based summary reports for rain gauge analysis results.

Functions:
    create_summary_report: Generate comprehensive analysis report

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


# Version info
__version__ = "1.0.0"


# =============================================================================
# Helper Functions
# =============================================================================

def _count_overflow_thresholds(traces: List[Dict[str, Any]]) -> int:
    """
    Count thresholds with category='Overflow'.
    
    Args:
        traces: List of trace dictionaries
        
    Returns:
        Number of overflow thresholds found
    """
    count = 0
    for trace in traces:
        thresholds = trace.get("thresholds", []) or []
        for threshold in thresholds:
            category = (threshold.get("category") or "").strip().lower()
            if category == "overflow":
                count += 1
    return count


def _has_any_alarms(alarms_by_type: Dict[str, Any]) -> bool:
    """
    Check if alarms_by_type has any non-empty alarm lists.
    
    Args:
        alarms_by_type: Dictionary of alarm lists by type
        
    Returns:
        True if any alarms exist, False otherwise
    """
    if not alarms_by_type or not isinstance(alarms_by_type, dict):
        return False
    
    for key, val in alarms_by_type.items():
        if isinstance(val, list) and len(val) > 0:
            return True
    
    return False


def _trace_has_config(trace: Dict[str, Any]) -> bool:
    """
    Check if trace has any alarm/threshold configuration.
    
    Args:
        trace: Trace dictionary
        
    Returns:
        True if trace has configuration, False otherwise
    """
    # Check thresholds
    thresholds = trace.get("thresholds", []) or []
    if thresholds:
        return True
    
    # Check alarms by type
    alarms_by_type = trace.get("alarms_by_type", {}) or {}
    if _has_any_alarms(alarms_by_type):
        return True
    
    # Check hasAlarms flag
    has_alarms = (trace.get("trace", {}) or {}).get("hasAlarms")
    if has_alarms:
        return True
    
    return False


def _has_primary_rainfall_telemetered(traces: List[Dict[str, Any]]) -> bool:
    """
    Check if gauge has primary rainfall trace with telemeteredMaximumTime.
    
    Args:
        traces: List of trace dictionaries
        
    Returns:
        True if primary rainfall trace with telemetry exists, False otherwise
    """
    for trace in traces:
        trace_obj = trace.get("trace", {}) or {}
        description = (trace_obj.get("description") or "").strip().lower()
        
        if description == "rainfall":
            telemetered_time = trace_obj.get("telemeteredMaximumTime")
            if telemetered_time:
                return True
    
    return False


def _extract_region_from_exclude_keyword(exclude_keyword: str) -> str:
    """
    Extract region name from exclude keyword pattern.
    
    Logic: If keyword excludes "northland|waikato", we're likely analyzing Auckland.
    
    Args:
        exclude_keyword: Regex pattern of excluded regions
        
    Returns:
        Best guess at region being analyzed
    """
    if not exclude_keyword:
        return "Active"
    
    # Common patterns
    lower_keyword = exclude_keyword.lower()
    
    # If excluding northland/waikato, likely Auckland
    if "northland" in lower_keyword and "waikato" in lower_keyword:
        return "Auckland"
    
    # If excluding auckland, likely another region
    if "auckland" in lower_keyword:
        return "Non-Auckland"
    
    # Generic fallback
    return "Active"


# =============================================================================
# Main Reporting Function
# =============================================================================

def create_summary_report(
    filtered_data: Dict[str, Any],
    alarms_df: pd.DataFrame,
    inactive_months: Optional[int] = None,
    exclude_keyword: Optional[str] = None,
) -> str:
    """
    Generate comprehensive analysis report for filtered gauge data.
    
    Args:
        filtered_data: Dictionary from filter_gauges() containing:
            - stats: Summary statistics
            - active_gauges: List of active gauges
            - inactive_gauges: List of inactive gauges
            - excluded_gauges: List of excluded gauges
        alarms_df: DataFrame with alarm/threshold configurations
        inactive_months: Inactivity threshold used (for report accuracy)
        exclude_keyword: Exclusion pattern used (for region identification)
        
    Returns:
        Formatted text report as string
        
    Example:
        >>> report = create_summary_report(
        ...     filtered_data=result,
        ...     alarms_df=alarms_df,
        ...     inactive_months=3,
        ...     exclude_keyword="northland|waikato"
        ... )
        >>> print(report)
    """
    # Extract data
    stats = filtered_data.get("stats", {})
    active_gauges = filtered_data.get("active_gauges", [])
    
    total = stats.get("total_gauges", 0)
    excluded = stats.get("excluded_gauges", 0)
    no_rainfall = stats.get("no_rainfall_trace", 0)
    inactive = stats.get("inactive_gauges", 0)
    active = stats.get("active_auckland_gauges", 0)
    
    # Determine region name from exclude keyword
    region_name = _extract_region_from_exclude_keyword(exclude_keyword or "")
    
    # Use provided inactive_months or try to infer from data
    if inactive_months is None:
        inactive_months = 3  # Default fallback
    
    # Build report
    lines = [
        "=" * 80,
        f"{region_name.upper()} RAIN GAUGE ANALYSIS REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        "",
        "FILTERING RESULTS:",
        f"  Total gauges in dataset: {total}",
        "",
        f"  Step 1 - Exclude non-{region_name} (by keyword): {excluded} removed",
        f"           Remaining: {total - excluded}",
        "",
        f"  Step 2 - Require physical sensor data: {no_rainfall} removed",
        f"           (gauges with only forecast/nowcast traces, no measured rainfall)",
        f"           Remaining: {total - excluded - no_rainfall}",
        "",
        f"  Step 3 - Require recent telemetered data: {inactive} removed",
        f"           (telemeteredMaximumTime missing or older than {inactive_months} months)",
        f"           Remaining: {total - excluded - no_rainfall - inactive}",
        "",
        f"  ✓ Active {region_name} rain gauges: {active}",
        "",
        "=" * 80,
        "ACTIVE GAUGE DETAILS:",
        "=" * 80,
        "",
    ]
    
    # Sort gauges by last data time (most recent first)
    sorted_gauges = sorted(
        active_gauges,
        key=lambda g: g.get("last_data_time_dt") or datetime.min,
        reverse=True,
    )
    
    # Add gauge details
    for gauge_data in sorted_gauges:
        gauge = gauge_data.get("gauge", {}) or {}
        name = gauge.get("name", "Unknown")
        gauge_id = gauge.get("id")
        last_dt = gauge_data.get("last_data_time_dt")
        
        traces = gauge_data.get("traces", []) or []
        
        # Count traces with any alarm/threshold config
        traces_with_config = sum(1 for t in traces if _trace_has_config(t))
        
        # Count overflow thresholds
        total_overflow = _count_overflow_thresholds(traces)
        
        # Count all thresholds
        total_thresholds = sum(
            len(t.get("thresholds", []) or []) for t in traces
        )
        
        # Check if has recency monitoring
        has_recency = 1 if _has_primary_rainfall_telemetered(traces) else 0
        
        # Format gauge info
        lines.append(f"• {name}")
        lines.append(f"  ID: {gauge_id}")
        lines.append(
            f"  Last data: {last_dt.strftime('%Y-%m-%d %H:%M:%S') if last_dt else 'Unknown'}"
        )
        lines.append(
            f"  Traces: {len(traces)} total, {traces_with_config} with alarms/thresholds"
        )
        lines.append(f"  Overflow thresholds: {total_overflow}")
        lines.append(f"  Recency monitoring: {has_recency}")
        lines.append(f"  Total alarm configs: {total_thresholds + has_recency}")
        lines.append("")
    
    # Alarm summary section
    lines.extend([
        "=" * 80,
        "ALARM & THRESHOLD CONFIGURATION SUMMARY:",
        "=" * 80,
        "",
    ])
    
    # Check if alarms exist
    if alarms_df is None or alarms_df.empty:
        lines.append("No alarm/threshold configurations found on active gauges.")
        lines.append("")
        lines.append("=" * 80)
        return "\n".join(lines)
    
    # Configuration sources
    if "source" in alarms_df.columns:
        lines.append("Configuration sources:")
        
        source_labels = {
            "overflow_alarm": "Overflow alarms",
            "threshold_config": "Threshold configs",
            "detailed_alarm": "Detailed alarms",
            "has_alarms_flag": "Has alarms flag",
            "derived_recency": "Derived recency",
            "trace_inventory": "Trace inventory",
            "alarm_inventory": "Alarm inventory",
        }
        
        for source, count in alarms_df["source"].value_counts().items():
            label = source_labels.get(source, source)
            lines.append(f"  {label}: {count}")
        
        lines.append("")
    
    # Count by alarm type
    if "alarm_type" in alarms_df.columns:
        lines.append("By alarm type:")
        
        for alarm_type, count in alarms_df["alarm_type"].value_counts().items():
            if alarm_type and str(alarm_type).strip():
                lines.append(f"  {alarm_type}: {count}")
        
        lines.append("")
    
    # Calculate totals
    total_overflow = 0
    total_recency = 0
    
    if "alarm_type" in alarms_df.columns:
        # Count overflow alarms (case-insensitive)
        total_overflow = len(
            alarms_df[alarms_df["alarm_type"].str.contains(
                "Overflow", case=False, na=False
            )]
        )
        
        # Count recency monitors
        total_recency = len(
            alarms_df[alarms_df["alarm_type"] == "Recency"]
        )
    
    # Total summary
    lines.append(f"Total overflow thresholds: {total_overflow}")
    lines.append(f"Total recency monitors: {total_recency}")
    lines.append(f"Total configured alarms: {total_overflow + total_recency}")
    lines.append("")
    lines.append("=" * 80)
    
    return "\n".join(lines)