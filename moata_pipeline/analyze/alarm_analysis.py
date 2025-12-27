"""
Alarm Analysis Module

Analyzes rain gauge alarm configurations and creates comprehensive DataFrames
for reporting and validation.

Key Functions:
    analyze_alarms: Main entry point - creates all_traces and alarms_only DataFrames
    
Helper Functions:
    Type casting: _as_str, _as_bool, _as_int
    Datetime: _parse_iso, _to_utc, _fmt_ddmmyyyy, _fmt_iso
    Normalizers: resolution_bucket, data_variable_type_fields, style_config_fields
    Alarm helpers: _get_alarm_name, alarms_by_type_inventory

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# Version info
__version__ = "1.0.0"


# =============================================================================
# Type Casting Helpers
# =============================================================================

def _as_str(x: Any) -> Optional[str]:
    """
    Safely cast value to string.
    
    Args:
        x: Value to cast
        
    Returns:
        String value or None if empty/invalid
    """
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _as_bool(x: Any) -> Optional[bool]:
    """
    Safely cast value to boolean.
    
    Handles: bool, int, float, str ("true", "yes", "1", etc.)
    
    Args:
        x: Value to cast
        
    Returns:
        Boolean value or None if cannot determine
    """
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        s = x.strip().lower()
        if s in {"true", "t", "1", "yes", "y"}:
            return True
        if s in {"false", "f", "0", "no", "n"}:
            return False
    return None


def _as_int(x: Any) -> Optional[int]:
    """
    Safely cast value to integer.
    
    Args:
        x: Value to cast
        
    Returns:
        Integer value or None if invalid
    """
    if x is None:
        return None
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return int(float(s))
        except Exception:
            return None
    return None


# =============================================================================
# Datetime Helpers
# =============================================================================

def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO datetime strings with various formats.
    
    Supports:
        - Timezone offsets: 2025-12-25T15:59:30+12:00
        - Z suffix: 2025-12-25T03:59:30Z
        - Naive: 2025-12-25T03:59:30 (assumed UTC)
        
    Args:
        dt_str: ISO datetime string
        
    Returns:
        Datetime object or None if invalid
    """
    if not dt_str:
        return None

    s = dt_str.strip()
    if not s:
        return None

    try:
        # Handle trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Convert datetime to UTC timezone.
    
    Args:
        dt: Datetime to convert
        
    Returns:
        UTC datetime or None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_ddmmyyyy(dt: Optional[datetime]) -> Optional[str]:
    """Format datetime as DD/MM/YYYY."""
    return dt.strftime("%d/%m/%Y") if dt else None


def _fmt_iso(dt: Optional[datetime]) -> Optional[str]:
    """Format datetime as ISO string."""
    return dt.isoformat() if dt else None


# =============================================================================
# Field Normalizers (for heterogeneous API data)
# =============================================================================

def resolution_bucket(res: Any) -> Optional[str]:
    """
    Normalize resolution field to stable string.
    
    Handles:
        - Numeric seconds: 300, 3600
        - String format: "5m", "1m"
        - Dict format: {"unit":"minute", "value":5}
        
    Args:
        res: Resolution value in various formats
        
    Returns:
        Normalized string or None
    """
    if res is None:
        return None

    if isinstance(res, dict):
        unit = _as_str(res.get("unit")) or "unit"
        val = res.get("value")
        return f"{val}{unit}"

    # If numeric (commonly seconds)
    if isinstance(res, (int, float)) and not isinstance(res, bool):
        r = _as_int(res)
        return str(r) if r is not None else _as_str(res)

    return _as_str(res)


def data_variable_type_fields(dvt_raw: Any) -> Dict[str, Any]:
    """
    Extract and normalize dataVariableType fields.
    
    Handles heterogeneous formats:
        - dict with id/name/description/type/units
        - plain id (numeric)
        - plain name (string)
        - null
        
    Args:
        dvt_raw: Raw dataVariableType value
        
    Returns:
        Dictionary with normalized fields
    """
    out: Dict[str, Any] = {
        "trace_var_type_id": None,
        "trace_var_type_name": None,
        "trace_var_type_description": None,
        "trace_var_type_type": None,
        "trace_var_type_is_zero_relevant": None,
        "trace_var_type_order": None,
        "trace_units_id": None,
        "trace_units_label": None,
        "trace_units_is_total_relevant": None,
    }

    if isinstance(dvt_raw, dict):
        out["trace_var_type_id"] = dvt_raw.get("id")
        out["trace_var_type_name"] = dvt_raw.get("name")
        out["trace_var_type_description"] = dvt_raw.get("description")
        out["trace_var_type_type"] = dvt_raw.get("type")
        out["trace_var_type_is_zero_relevant"] = dvt_raw.get("isZeroRelevant")
        out["trace_var_type_order"] = dvt_raw.get("order")

        units = dvt_raw.get("units") or {}
        if isinstance(units, dict):
            out["trace_units_id"] = units.get("id")
            out["trace_units_label"] = units.get("label")
            out["trace_units_is_total_relevant"] = units.get("isTotalRelevant")
        return out

    # Fallback: store as name if string, id if numeric
    if isinstance(dvt_raw, str):
        out["trace_var_type_name"] = dvt_raw
    elif isinstance(dvt_raw, (int, float)) and not isinstance(dvt_raw, bool):
        out["trace_var_type_id"] = _as_int(dvt_raw)

    return out


def style_config_fields(sc_raw: Any) -> Dict[str, Any]:
    """
    Extract and normalize styleConfig fields.
    
    Handles formats:
        - {"all": {"type":..., "color":..., "attach":...}}
        - {"type":..., "invert":...}
        - null
        
    Args:
        sc_raw: Raw styleConfig value
        
    Returns:
        Dictionary with normalized style fields
    """
    out = {
        "trace_style_type": None,
        "trace_style_color": None,
        "trace_style_attach": None,
        "trace_style_invert": None,
    }
    
    if not isinstance(sc_raw, dict):
        return out

    if "all" in sc_raw and isinstance(sc_raw.get("all"), dict):
        sc_all = sc_raw.get("all") or {}
        out["trace_style_type"] = sc_all.get("type")
        out["trace_style_color"] = sc_all.get("color")
        out["trace_style_attach"] = sc_all.get("attach")
        out["trace_style_invert"] = sc_all.get("invert")
        return out

    # Flat shape
    out["trace_style_type"] = sc_raw.get("type")
    out["trace_style_invert"] = sc_raw.get("invert")
    
    return out


def virtual_trace_fields(vt_raw: Any) -> Dict[str, Any]:
    """
    Extract and normalize virtualTrace fields.
    
    Handles formats:
        - bool true/false
        - dict with {id, description, parentTraceId}
        - null
        
    Args:
        vt_raw: Raw virtualTrace value
        
    Returns:
        Dictionary with normalized virtual trace fields
    """
    out = {
        "trace_virtual_bool": None,
        "trace_virtual_id": None,
        "trace_virtual_description": None,
        "trace_virtual_parent_trace_id": None,
    }

    if isinstance(vt_raw, dict):
        out["trace_virtual_bool"] = True
        out["trace_virtual_id"] = vt_raw.get("id")
        out["trace_virtual_description"] = vt_raw.get("description")
        out["trace_virtual_parent_trace_id"] = vt_raw.get("parentTraceId")
        return out

    b = _as_bool(vt_raw)
    if b is not None:
        out["trace_virtual_bool"] = b

    return out


# =============================================================================
# Alarm Analysis Helpers
# =============================================================================

def _get_alarm_name(th: Dict[str, Any]) -> str:
    """
    Get descriptive alarm name from threshold config.
    
    Falls back to alarmDescription if name is too short (e.g., "mm").
    
    Args:
        th: Threshold dictionary
        
    Returns:
        Alarm name string
    """
    alarm_name = (th.get("name") or "").strip()

    if len(alarm_name) < 4:
        alarm_desc = th.get("alarmDescription", "")
        if alarm_desc:
            if " measured at " in alarm_desc:
                alarm_name = alarm_desc.split(" measured at ")[0]
            else:
                alarm_name = alarm_desc

    return alarm_name if alarm_name else "Unknown Threshold"


def alarms_by_type_inventory(trace_wrap: Dict[str, Any]) -> Tuple[List[str], Dict[str, int]]:
    """
    Determine which alarm types are present based on alarms_by_type payload.
    
    Args:
        trace_wrap: Trace wrapper dictionary
        
    Returns:
        Tuple of (list of alarm types present, counts by type)
    """
    present: List[str] = []
    counts: Dict[str, int] = {}

    alarms_by_type = trace_wrap.get("alarms_by_type")
    if not isinstance(alarms_by_type, dict):
        return present, counts

    for alarm_type, payload in alarms_by_type.items():
        if alarm_type is None:
            continue
        k = _as_str(alarm_type)
        if not k:
            continue

        n = 0
        if isinstance(payload, list):
            n = len(payload)
        elif isinstance(payload, dict):
            n = len(payload.keys())
        elif payload is None:
            n = 0
        else:
            n = 1  # Some other scalar payload

        if n > 0:
            present.append(k)
            counts[k] = n

    present = sorted(set(present))
    return present, counts


def detailed_alarm_inventory(trace_wrap: Dict[str, Any]) -> Optional[str]:
    """
    Extract human-readable alarm type from detailed_alarm.
    
    Args:
        trace_wrap: Trace wrapper dictionary
        
    Returns:
        Alarm type string or None
    """
    da = trace_wrap.get("detailed_alarm")
    if not isinstance(da, dict) or not da:
        return None

    for key in ["alarmType", "type", "name", "description"]:
        v = _as_str(da.get(key))
        if v:
            return v

    return "present"


def json_dumps_safe(obj: Any) -> Optional[str]:
    """
    Safely serialize object to JSON string.
    
    Args:
        obj: Object to serialize
        
    Returns:
        JSON string or None if serialization fails
    """
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return None


# =============================================================================
# Trace Extraction
# =============================================================================

def _extract_trace_fields(trace: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract trace fields with heterogeneity guards.
    
    Args:
        trace: Trace dictionary
        
    Returns:
        Dictionary with normalized trace fields
    """
    result: Dict[str, Any] = {
        "trace_id": trace.get("id"),
        "trace_asset_id": trace.get("assetId"),
        "trace_description": trace.get("description"),
        "trace_has_alarms": _as_bool(trace.get("hasAlarms")),
        "trace_is_visible": _as_bool(trace.get("isVisible")),
        "trace_resolution_raw": trace.get("resolution"),
        "trace_resolution": resolution_bucket(trace.get("resolution")),
        "trace_timezone": _as_str(trace.get("timeZone")),
        # Time fields
        "trace_telemetered_max_time": trace.get("telemeteredMaximumTime"),
        "trace_archived_min_time": trace.get("archivedMinimumTime"),
        "trace_archived_max_time": trace.get("archivedMaximumTime"),
        "trace_forecasted_min_time": trace.get("forecastedMinimumTime"),
        "trace_forecasted_max_time": trace.get("forecastedMaximumTime"),
    }

    # dataVariableType
    dvt_raw = trace.get("dataVariableType")
    result.update(data_variable_type_fields(dvt_raw))

    # virtualTrace
    result.update(virtual_trace_fields(trace.get("virtualTrace")))

    # styleConfig
    result.update(style_config_fields(trace.get("styleConfig")))

    return result


# =============================================================================
# Trace Selection Helpers
# =============================================================================

def _is_primary_rainfall_trace(trace: Dict[str, Any]) -> bool:
    """Check if trace is primary rainfall trace."""
    desc = (trace.get("description") or "").strip().lower()
    return desc == "rainfall"


def _get_latest_telemetered_time(traces: List[Dict[str, Any]]) -> Optional[datetime]:
    """Get latest telemetered time across all traces."""
    latest: Optional[datetime] = None
    for td in traces:
        t = td.get("trace", {}) or {}
        tmax = _to_utc(_parse_iso(_as_str(t.get("telemeteredMaximumTime"))))
        if tmax and (latest is None or tmax > latest):
            latest = tmax
    return latest


def _get_primary_rainfall_times(
    traces: List[Dict[str, Any]],
) -> Tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
    """
    Get archived min/max and telemetered max from primary rainfall trace.
    
    Returns:
        Tuple of (archived_min, archived_max, telemetered_max)
    """
    for td in traces:
        t = td.get("trace", {}) or {}
        if _is_primary_rainfall_trace(t):
            amin = _to_utc(_parse_iso(_as_str(t.get("archivedMinimumTime"))))
            amax = _to_utc(_parse_iso(_as_str(t.get("archivedMaximumTime"))))
            tmax = _to_utc(_parse_iso(_as_str(t.get("telemeteredMaximumTime"))))
            return amin, amax, tmax
    return None, None, None


# =============================================================================
# Main Analysis Function
# =============================================================================

def analyze_alarms(
    active_gauges: List[Dict[str, Any]],
    *,
    inactive_threshold_months: int = 3,
    now_utc: Optional[datetime] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build comprehensive alarm analysis DataFrames from gauge data.
    
    Creates two DataFrames:
        1. all_traces_df: Complete inventory of all traces and configurations
        2. alarms_only_df: Filtered to only alarm/threshold configurations
        
    all_traces_df includes:
        - 1 derived recency row per gauge (source=derived_recency)
        - 1 trace inventory row per trace (source=trace_inventory)
        - 1 row per threshold config (source=threshold_config)
        
    alarms_only_df includes:
        - Derived recency rows
        - Threshold config rows
        - Alarm inventory rows (if alarms_by_type/detailed_alarm present)
        
    Args:
        active_gauges: List of active gauge dictionaries from filter_gauges()
        inactive_threshold_months: Months threshold for recency calculation
        now_utc: Current time (default: datetime.now(timezone.utc))
        
    Returns:
        Tuple of (all_traces_df, alarms_only_df)
        
    Example:
        >>> all_df, alarms_df = analyze_alarms(
        ...     active_gauges,
        ...     inactive_threshold_months=3
        ... )
        >>> print(f"Total traces: {len(all_df)}")
        >>> print(f"Alarms found: {len(alarms_df)}")
    """
    all_records: List[Dict[str, Any]] = []
    alarm_records: List[Dict[str, Any]] = []

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    inactive_threshold_days = inactive_threshold_months * 30

    for gauge_data in active_gauges:
        gauge = gauge_data.get("gauge", {}) or {}
        gauge_id = gauge.get("id")
        gauge_name = gauge.get("name", "Unknown")
        gauge_description = gauge.get("description")
        gauge_project_id = gauge.get("projectId")
        gauge_asset_type = gauge.get("assetType")
        gauge_asset_types = gauge.get("assetTypes")
        gauge_last_modified = gauge.get("lastModified")
        gauge_modified_by = gauge.get("modifiedBy")

        traces = gauge_data.get("traces", []) or []
        if not isinstance(traces, list):
            traces = []

        # Primary Rainfall times
        archived_min, archived_max, telem_max_primary = _get_primary_rainfall_times(traces)

        # Fallback for last_data display
        last_dt = telem_max_primary or _get_latest_telemetered_time(traces)
        last_data_str = _fmt_ddmmyyyy(last_dt)

        # Derived recency metrics
        telem_max_for_age = telem_max_primary or last_dt
        recency_age_hours: Optional[float] = None
        recency_age_days: Optional[float] = None
        is_active_by_months: Optional[bool] = None

        if telem_max_for_age:
            delta_sec = (now_utc - telem_max_for_age).total_seconds()
            recency_age_hours = round(delta_sec / 3600.0, 2)
            recency_age_days = round(delta_sec / 86400.0, 2)
            is_active_by_months = (delta_sec <= inactive_threshold_days * 86400.0)

        # Base gauge fields (repeatable for every row)
        gauge_base = {
            "gauge_id": gauge_id,
            "gauge_name": gauge_name,
            "gauge_description": gauge_description,
            "gauge_project_id": gauge_project_id,
            "gauge_asset_type": gauge_asset_type,
            "gauge_asset_types": str(gauge_asset_types) if gauge_asset_types else None,
            "gauge_last_modified": gauge_last_modified,
            "gauge_modified_by": gauge_modified_by,
            "last_data": last_data_str,
            "recency_age_hours": recency_age_hours,
            "recency_age_days": recency_age_days,
            "is_active_by_months": is_active_by_months,
        }

        # Template for alarm-related columns
        alarm_cols_empty = {
            "alarm_id": None,
            "alarm_name": None,
            "alarm_type": None,
            "alarm_description": None,
            "threshold": None,
            "threshold_type": None,
            "threshold_category": None,
            "threshold_category_id": None,
            "severity": None,
            "is_critical": None,
            "alarm_types_present": None,
            "alarm_types_present_count": None,
            "alarm_types_counts_json": None,
            "detailed_alarm_type": None,
            "source": None,
        }

        # (A) Derived Recency (1 per gauge)
        recency_row = {
            **gauge_base,
            # Trace fields blanked
            "trace_id": None,
            "trace_asset_id": None,
            "trace_description": "Rainfall (primary)" if telem_max_primary else "Rainfall (primary not found)",
            "trace_has_alarms": None,
            "trace_is_visible": None,
            "trace_resolution_raw": None,
            "trace_resolution": None,
            "trace_timezone": None,
            "trace_var_type_id": None,
            "trace_var_type_name": None,
            "trace_var_type_description": None,
            "trace_var_type_type": None,
            "trace_var_type_is_zero_relevant": None,
            "trace_var_type_order": None,
            "trace_units_id": None,
            "trace_units_label": None,
            "trace_units_is_total_relevant": None,
            "trace_telemetered_max_time": _fmt_iso(telem_max_for_age),
            "trace_archived_min_time": _fmt_iso(archived_min),
            "trace_archived_max_time": _fmt_iso(archived_max),
            "trace_forecasted_min_time": None,
            "trace_forecasted_max_time": None,
            "trace_virtual_bool": None,
            "trace_virtual_id": None,
            "trace_virtual_description": None,
            "trace_virtual_parent_trace_id": None,
            "trace_style_type": None,
            "trace_style_color": None,
            "trace_style_attach": None,
            "trace_style_invert": None,
            # Alarm columns
            **alarm_cols_empty,
            "alarm_name": "Data Recency (derived from telemeteredMaximumTime)",
            "alarm_type": "Recency",
            "threshold": recency_age_hours,
            "source": "derived_recency",
        }
        all_records.append(recency_row)
        alarm_records.append(recency_row)

        # (B) Per-trace inventory + thresholds + alarms inventory
        for trace_wrap in traces:
            if not isinstance(trace_wrap, dict):
                continue

            trace = trace_wrap.get("trace", {}) or {}
            if not isinstance(trace, dict):
                trace = {}

            trace_fields = _extract_trace_fields(trace)

            # 1) Trace inventory row
            present_alarm_types, alarm_type_counts = alarms_by_type_inventory(trace_wrap)
            detailed_type = detailed_alarm_inventory(trace_wrap)

            trace_inventory_row = {
                **gauge_base,
                **trace_fields,
                **alarm_cols_empty,
                "alarm_types_present": ",".join(present_alarm_types) if present_alarm_types else None,
                "alarm_types_present_count": len(present_alarm_types) if present_alarm_types else 0,
                "alarm_types_counts_json": json_dumps_safe(alarm_type_counts) if alarm_type_counts else None,
                "detailed_alarm_type": detailed_type,
                "source": "trace_inventory",
            }
            all_records.append(trace_inventory_row)

            # 2) Alarm inventory row (if alarms exist)
            if (present_alarm_types and len(present_alarm_types) > 0) or (detailed_type is not None):
                inv_alarm_row = {
                    **trace_inventory_row,
                    "alarm_name": "Alarm inventory (from alarms_by_type / detailed_alarm)",
                    "alarm_type": "AlarmInventory",
                    "source": "alarm_inventory",
                }
                alarm_records.append(inv_alarm_row)

            # 3) Threshold configs (one row per threshold)
            thresholds = trace_wrap.get("thresholds", []) or []
            if isinstance(thresholds, dict):
                thresholds = list(thresholds.values())

            if isinstance(thresholds, list) and thresholds:
                for th in thresholds:
                    if not isinstance(th, dict):
                        continue

                    threshold_value = th.get("value", th.get("thresholdValue"))
                    category = th.get("category", "Threshold")
                    threshold_type = th.get("thresholdType")
                    alarm_type = f"{category}/{threshold_type}" if threshold_type else str(category)
                    alarm_name = _get_alarm_name(th)

                    threshold_row = {
                        **gauge_base,
                        **trace_fields,
                        **alarm_cols_empty,
                        "alarm_id": th.get("id"),
                        "alarm_name": alarm_name,
                        "alarm_type": alarm_type,
                        "alarm_description": th.get("alarmDescription"),
                        "threshold": threshold_value,
                        "threshold_type": threshold_type,
                        "threshold_category": category,
                        "threshold_category_id": th.get("thresholdCategoryId"),
                        "severity": th.get("severity"),
                        "is_critical": th.get("isCritical"),
                        # Keep inventory context
                        "alarm_types_present": ",".join(present_alarm_types) if present_alarm_types else None,
                        "alarm_types_present_count": len(present_alarm_types) if present_alarm_types else 0,
                        "alarm_types_counts_json": json_dumps_safe(alarm_type_counts) if alarm_type_counts else None,
                        "detailed_alarm_type": detailed_type,
                        "source": "threshold_config",
                    }
                    all_records.append(threshold_row)
                    alarm_records.append(threshold_row)

    all_traces_df = pd.DataFrame(all_records)
    alarms_only_df = pd.DataFrame(alarm_records)
    
    return all_traces_df, alarms_only_df