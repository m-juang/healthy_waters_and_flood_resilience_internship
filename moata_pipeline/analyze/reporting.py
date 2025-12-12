from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import pandas as pd


def create_summary_report(filtered_data: Dict[str, Any], alarms_df: pd.DataFrame) -> str:
    stats = filtered_data["stats"]
    active_gauges = filtered_data["active_gauges"]

    lines = [
        "=" * 80,
        "AUCKLAND RAIN GAUGE ANALYSIS REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        "",
        "FILTERING RESULTS:",
        f"  Total gauges in dataset: {stats['total_gauges']}",
        f"  ✓ Active Auckland gauges: {stats['active_auckland_gauges']}",
        f"  ✗ Inactive gauges (>threshold): {stats['inactive_gauges']}",
        f"  ✗ Excluded gauges: {stats['excluded_gauges']}",
        f"  ✗ No Rainfall trace: {stats['no_rainfall_trace']}",
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
        traces_with_cfg = sum(
            1
            for t in traces
            if (t.get("overflow_alarms") or t.get("thresholds") or (t.get("trace", {}) or {}).get("hasAlarms"))
        )
        total_overflow = sum(len(t.get("overflow_alarms", []) or []) for t in traces)
        total_thresholds = sum(len(t.get("thresholds", []) or []) for t in traces)

        lines.append(f"• {name}")
        lines.append(f"  ID: {gid}")
        lines.append(f"  Last data: {last_dt.strftime('%Y-%m-%d %H:%M:%S') if last_dt else 'Unknown'}")
        lines.append(f"  Traces: {len(traces)} total, {traces_with_cfg} with alarms/thresholds")
        lines.append(f"  Overflow alarms: {total_overflow}")
        lines.append(f"  Threshold configs: {total_thresholds}")
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
            }.get(source, source)
            lines.append(f"  {label}: {count}")
        lines.append("")

    alarms_with_id = alarms_df[alarms_df.get("alarm_id").notna()] if "alarm_id" in alarms_df.columns else alarms_df
    lines.append(f"Total configured alarms/thresholds: {len(alarms_with_id)}")
    lines.append("")

    if "trace_name" in alarms_with_id.columns:
        by_trace = alarms_with_id.groupby("trace_name").size().sort_values(ascending=False)
        lines.append("Alarms/thresholds by trace type:")
        for tn, cnt in by_trace.items():
            lines.append(f"  {tn}: {cnt}")
        lines.append("")

    if "threshold" in alarms_with_id.columns and "trace_name" in alarms_with_id.columns:
        lines.append("Threshold ranges by trace type (numeric-only):")
        for tn in alarms_with_id["trace_name"].dropna().unique():
            tdf = alarms_with_id[alarms_with_id["trace_name"] == tn]
            th = pd.to_numeric(tdf["threshold"], errors="coerce").dropna()
            if th.empty:
                lines.append(f"  {tn}: (no numeric thresholds)")
            else:
                lines.append(f"  {tn}: {th.min():.2f} - {th.max():.2f}")
        lines.append("")

    lines.append("=" * 80)
    return "\n".join(lines)
