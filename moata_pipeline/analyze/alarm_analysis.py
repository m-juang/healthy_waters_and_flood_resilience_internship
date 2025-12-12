from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd


def analyze_alarms(active_gauges: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Build the same alarm_summary dataframe you had, including 'thresholds' field.
    """
    records: List[Dict[str, Any]] = []

    for gauge_data in active_gauges:
        gauge = gauge_data.get("gauge", {}) or {}
        gauge_name = gauge.get("name", "Unknown")
        gauge_id = gauge.get("id")
        last_dt = gauge_data.get("last_data_time_dt")

        traces = gauge_data.get("traces", []) or []

        for trace_data in traces:
            trace = trace_data.get("trace", {}) or {}
            trace_name = trace.get("description", "Unknown")
            trace_id = trace.get("id")
            has_alarms_flag = bool(trace.get("hasAlarms", False))

            overflow_alarms = trace_data.get("overflow_alarms", []) or []
            detailed_alarm = trace_data.get("detailed_alarm")
            thresholds = trace_data.get("thresholds", []) or []

            # Pull some info from detailed_alarm if present
            detailed_threshold = None
            detailed_severity = None
            alarm_type = None

            if detailed_alarm:
                alarm_type = detailed_alarm.get("alarmType", "Unknown")
                alarm_thresholds = detailed_alarm.get("alarmThresholds", []) or []
                if alarm_thresholds:
                    first = alarm_thresholds[0] or {}
                    detailed_threshold = first.get("thresholdValue")
                    detailed_severity = first.get("alarmSeverity")

                if alarm_type == "DataRecency":
                    detailed_threshold = detailed_alarm.get("maxLookbackOverride", detailed_threshold)

            # 1) Overflow alarms rows
            for alarm in overflow_alarms:
                records.append(
                    {
                        "gauge_id": gauge_id,
                        "gauge_name": gauge_name,
                        "last_data": last_dt.strftime("%Y-%m-%d") if last_dt else None,
                        "trace_id": trace_id,
                        "trace_name": trace_name,
                        "alarm_id": alarm.get("id"),
                        "alarm_name": alarm.get("name", ""),
                        "alarm_type": "OverflowMonitoring",
                        "threshold": alarm.get("threshold", detailed_threshold),
                        "severity": alarm.get("severity", detailed_severity),
                        "is_critical": alarm.get("isCritical"),
                        "source": "overflow_alarm",
                    }
                )

            # 2) Threshold configs rows
            for th in thresholds:
                records.append(
                    {
                        "gauge_id": gauge_id,
                        "gauge_name": gauge_name,
                        "last_data": last_dt.strftime("%Y-%m-%d") if last_dt else None,
                        "trace_id": trace_id,
                        "trace_name": trace_name,
                        "alarm_id": th.get("id"),
                        "alarm_name": th.get("name", "Unknown Threshold"),
                        "alarm_type": th.get("type", th.get("category", "Threshold")),
                        "threshold": th.get("value") or th.get("thresholdValue"),
                        "severity": th.get("severity"),
                        "is_critical": th.get("isCritical"),
                        "source": "threshold_config",
                    }
                )

            # 3) Detailed alarm rows (if has alarms but no overflow alarms)
            if (not overflow_alarms) and has_alarms_flag:
                if detailed_alarm:
                    records.append(
                        {
                            "gauge_id": gauge_id,
                            "gauge_name": gauge_name,
                            "last_data": last_dt.strftime("%Y-%m-%d") if last_dt else None,
                            "trace_id": trace_id,
                            "trace_name": trace_name,
                            "alarm_id": detailed_alarm.get("alarmId"),
                            "alarm_name": detailed_alarm.get("description", f"{alarm_type} Alarm"),
                            "alarm_type": alarm_type,
                            "threshold": detailed_threshold,
                            "severity": detailed_severity,
                            "is_critical": None,
                            "source": "detailed_alarm",
                        }
                    )
                else:
                    records.append(
                        {
                            "gauge_id": gauge_id,
                            "gauge_name": gauge_name,
                            "last_data": last_dt.strftime("%Y-%m-%d") if last_dt else None,
                            "trace_id": trace_id,
                            "trace_name": trace_name,
                            "alarm_id": None,
                            "alarm_name": "Has alarms (recency)",
                            "alarm_type": "DataRecency",
                            "threshold": None,
                            "severity": None,
                            "is_critical": None,
                            "source": "has_alarms_flag",
                        }
                    )

    return pd.DataFrame(records)
