from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from moata_pipeline.common.time_utils import months_ago, now_like, parse_datetime


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilterConfig:
    inactive_threshold_months: int = 3
    exclude_keyword: str = "northland"  # default logic from your script


def is_auckland_gauge(gauge_name: str, exclude_keyword: str = "northland") -> bool:
    name = gauge_name or ""
    # exclude_keyword supports regex like "northland|waikato"
    return re.search(exclude_keyword, name, flags=re.IGNORECASE) is None


def get_rainfall_trace(traces_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Return the trace_data entry (not just trace) that represents the primary Rainfall trace.
    Input expected shape: trace_data = {"trace": {...}, "overflow_alarms": [...], ...}
    """
    # 1) Exact match on description == "Rainfall"
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        if description == "Rainfall":
            return trace_data

    # 2) dataVariableType name/type heuristics
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        dvt = trace.get("dataVariableType", {}) or {}
        dvt_name = (dvt.get("name") or "").strip()
        dvt_type = (dvt.get("type") or "").strip()

        if dvt_name == "Rainfall" or dvt_type == "Rain":
            if "rainfall" in description.lower() and "filtered" not in description.lower():
                return trace_data

    # 3) Fallback: any rainfall-like trace excluding mirrors
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        if "rainfall" in description.lower() and "mirror" not in description.lower():
            return trace_data

    return None


def is_gauge_active(telemetered_time: Optional[datetime], inactive_months: int) -> bool:
    """
    Active = telemeteredMaximumTime within last inactive_months months.
    Uses 30-day month approximation (consistent with your original script).
    """
    if telemetered_time is None:
        return False
    cutoff = months_ago(telemetered_time, inactive_months)
    return telemetered_time >= cutoff


def filter_gauges(all_data: List[Dict[str, Any]], cfg: FilterConfig) -> Dict[str, Any]:
    """
    Returns:
      {
        "active_gauges": [...],
        "inactive_gauges": [...],
        "excluded_gauges": [...],   # Northland (or other exclude keyword)
        "no_rainfall_trace": [...],
        "stats": {...}
      }
    """
    active_gauges: List[Dict[str, Any]] = []
    inactive_gauges: List[Dict[str, Any]] = []
    excluded_gauges: List[Dict[str, Any]] = []
    no_rainfall_trace: List[Dict[str, Any]] = []

    total = len(all_data)
    logger.info("Processing %d gauges...", total)

    for gauge_data in all_data:
        gauge = gauge_data.get("gauge", {}) or {}
        gauge_name = gauge.get("name", "Unknown")
        gauge_id = gauge.get("id")

        # Exclude non-Auckland (Northland)
        if not is_auckland_gauge(gauge_name, exclude_keyword=cfg.exclude_keyword):
            excluded_gauges.append(gauge_data)
            continue

        rainfall_trace_data = get_rainfall_trace(gauge_data.get("traces", []) or [])
        if not rainfall_trace_data:
            logger.warning("No 'Rainfall' trace found for gauge: %s (id=%s)", gauge_name, gauge_id)
            no_rainfall_trace.append(gauge_data)
            continue

        rainfall_trace = rainfall_trace_data.get("trace", {}) or {}
        telem_str = rainfall_trace.get("telemeteredMaximumTime")
        telem_dt = parse_datetime(telem_str)

        if telem_dt is None:
            # Missing or unparsable => treat inactive (consistent with your original script)
            logger.warning(
                "No/invalid telemeteredMaximumTime for gauge: %s (id=%s) - trace description: %s",
                gauge_name,
                gauge_id,
                rainfall_trace.get("description"),
            )
            inactive_gauges.append(gauge_data)
            continue

        if is_gauge_active(telem_dt, cfg.inactive_threshold_months):
            # Attach enrichment fields (same as your script)
            gauge_data["last_data_time"] = telem_dt.isoformat()
            gauge_data["last_data_time_dt"] = telem_dt
            gauge_data["rainfall_trace"] = rainfall_trace
            active_gauges.append(gauge_data)
            logger.info("✓ Active: %s (last: %s)", str(gauge_name)[:60], telem_dt.strftime("%Y-%m-%d"))
        else:
            inactive_gauges.append(gauge_data)

    stats = {
        "total_gauges": total,
        "active_auckland_gauges": len(active_gauges),
        "inactive_gauges": len(inactive_gauges),
        "excluded_gauges": len(excluded_gauges),
        "no_rainfall_trace": len(no_rainfall_trace),
    }

    logger.info("Filtering complete: %s", stats)

    return {
        "active_gauges": active_gauges,
        "inactive_gauges": inactive_gauges,
        "excluded_gauges": excluded_gauges,
        "no_rainfall_trace": no_rainfall_trace,
        "stats": stats,
    }
