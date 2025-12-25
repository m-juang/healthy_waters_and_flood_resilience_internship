from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


def _count_overflow_thresholds(traces: List[Dict[str, Any]]) -> int:
    """Count thresholds with category='Overflow'."""
    count = 0
    for t in traces:
        thresholds = t.get("thresholds", []) or []
        for th in thresholds:
            category = (th.get("category") or "").strip().lower()
            if category == "overflow":
                count += 1
    return count


def _has_any_alarms(alarms_by_type: Dict[str, Any]) -> bool:
    """Check if alarms_by_type has any non-empty alarm lists."""
    if not alarms_by_type or not isinstance(alarms_by_type, dict):
        return False
    for key, val in alarms_by_type.items():
        if isinstance(val, list) and len(val) > 0:
            return True
    return False


def _trace_has_config(t: Dict[str, Any]) -> bool:
    """Check if trace has any alarm/threshold configuration."""
    thresholds = t.get("thresholds", []) or []
    if thresholds:
        return True
    
    alarms_by_type = t.get("alarms_by_type", {}) or {}
    if _has_any_alarms(alarms_by_type):
        return True
    
    has_alarms = (t.get("trace", {}) or {}).get("hasAlarms")
    if has_alarms:
        return True
    
    return False


def _has_primary_rainfall_telemetered(traces: List[Dict[str, Any]]) -> bool:
    """Check if gauge has primary rainfall trace with telemeteredMaximumTime."""
    for t in traces:
        trace = t.get("trace", {}) or {}
        desc = (trace.get("description") or "").strip().lower()
        if desc == "rainfall":
            telem = trace.get("telemeteredMaximumTime")
            if telem:
                return True
    return False


def create_summary_report(filtered_data: Dict[str, Any], alarms_df: pd.DataFrame) -> str:
    stats = filtered_data["stats"]
    active_gauges = filtered_data["active_gauges"]

    total = stats["total_gauges"]
    excluded = stats["excluded_gauges"]
    no_rainfall = stats["no_rainfall_trace"]
    inactive = stats["inactive_gauges"]
    active = stats["active_auckland_gauges"]

    lines = [
        "=" * 80,
        "AUCKLAND RAIN GAUGE ANALYSIS REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        "",
        "FILTERING RESULTS:",
        f"  Total gauges in dataset: {total}",
        "",
        f"  Step 1 - Exclude non-Auckland (by keyword): {excluded} removed",
        f"           Remaining: {total - excluded}",
        "",
        f"  Step 2 - Require physical sensor data: {no_rainfall} removed",
        f"           (gauges with only forecast/nowcast traces, no measured rainfall)",
        f"           Remaining: {total - excluded - no_rainfall}",
        "",
        f"  Step 3 - Require recent telemetered data: {inactive} removed",
        f"           (telemeteredMaximumTime missing or older than 3 months)",
        f"           Remaining: {total - excluded - no_rainfall - inactive}",
        "",
        f"  ✓ Active Auckland rain gauges: {active}",
        "",
        "=" * 80,
        "ACTIVE GAUGE DETAILS:",
        "=" * 80,
        "",
    ]

    sorted_gauges = sorted(
        active_gauges,
        key=lambda g: g.get("last_data_time_dt") or datetime.min,
        reverse=True,
    )

    for g in sorted_gauges:
        gauge = g.get("gauge", {}) or {}
        name = gauge.get("name", "Unknown")
        gid = gauge.get("id")
        last_dt = g.get("last_data_time_dt")

        traces = g.get("traces", []) or []

        # Count traces with any alarm/threshold config
        traces_with_cfg = sum(1 for t in traces if _trace_has_config(t))

        # Count overflow thresholds (category='Overflow')
        total_overflow = _count_overflow_thresholds(traces)

        # Count all thresholds
        total_thresholds = sum(len(t.get("thresholds", []) or []) for t in traces)

        # Check if has recency monitoring (derived from primary rainfall telemeteredMaximumTime)
        has_recency = 1 if _has_primary_rainfall_telemetered(traces) else 0

        lines.append(f"• {name}")
        lines.append(f"  ID: {gid}")
        lines.append(f"  Last data: {last_dt.strftime('%Y-%m-%d %H:%M:%S') if last_dt else 'Unknown'}")
        lines.append(f"  Traces: {len(traces)} total, {traces_with_cfg} with alarms/thresholds")
        lines.append(f"  Overflow thresholds: {total_overflow}")
        lines.append(f"  Recency monitoring: {has_recency}")
        lines.append(f"  Total alarm configs: {total_thresholds + has_recency}")
        lines.append("")

    lines.extend(
        [
            "=" * 80,
            "ALARM & THRESHOLD CONFIGURATION SUMMARY:",
            "=" * 80,
            "",
        ]
    )

    if alarms_df is None or alarms_df.empty:
        lines.append("No alarm/threshold configurations found on active gauges.")
        lines.append("")
        lines.append("=" * 80)
        return "\n".join(lines)

    if "source" in alarms_df.columns:
        lines.append("Configuration sources:")
        for source, count in alarms_df["source"].value_counts().items():
            label = {
                "overflow_alarm": "Overflow alarms",
                "threshold_config": "Threshold configs",
                "detailed_alarm": "Detailed alarms",
                "has_alarms_flag": "Has alarms flag",
                "derived_recency": "Derived recency",
                "trace_inventory": "Trace inventory",
                "alarm_inventory": "Alarm inventory",
            }.get(source, source)
            lines.append(f"  {label}: {count}")
        lines.append("")

    # Count by alarm type
    if "alarm_type" in alarms_df.columns:
        lines.append("By alarm type:")
        for atype, count in alarms_df["alarm_type"].value_counts().items():
            if atype and str(atype).strip():
                lines.append(f"  {atype}: {count}")
        lines.append("")

    # Total summary
    total_overflow = (
        len(alarms_df[alarms_df["alarm_type"].str.contains("Overflow", case=False, na=False)])
        if "alarm_type" in alarms_df.columns
        else 0
    )
    total_recency = (
        len(alarms_df[alarms_df["alarm_type"] == "Recency"])
        if "alarm_type" in alarms_df.columns
        else 0
    )

    lines.append(f"Total overflow thresholds: {total_overflow}")
    lines.append(f"Total recency monitors: {total_recency}")
    lines.append(f"Total configured alarms: {total_overflow + total_recency}")
    lines.append("")

    lines.append("=" * 80)
    return "\n".join(lines)