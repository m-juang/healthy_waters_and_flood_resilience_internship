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
    # supports regex like "northland|waikato"
    exclude_keyword: str = "northland|waikato"


def is_auckland_gauge(gauge_name: str, exclude_keyword: str) -> bool:
    name = gauge_name or ""
    return re.search(exclude_keyword, name, flags=re.IGNORECASE) is None


def _is_bad_primary_rain_trace(description: str) -> bool:
    """
    Exclude non-primary rainfall traces that can make inactive gauges appear active.
    """
    d = (description or "").lower()
    bad_tokens = ("forecast", "nowcast", "merged", "anomaly", "filtered", "mirror")
    return any(t in d for t in bad_tokens)


def get_rainfall_trace(traces_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Return the trace_data entry that represents the PRIMARY measured Rainfall trace.

    Input expected shape:
      trace_data = {"trace": {...}, "overflow_alarms": [...], ...}
    """

    # 1) Exact match on description == "Rainfall" (best signal)
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        if description == "Rainfall" and not _is_bad_primary_rain_trace(description):
            return trace_data

    # 2) Strong heuristic: dataVariableType name Rain/Rainfall AND description looks like measured rainfall
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        if _is_bad_primary_rain_trace(description):
            continue

        dvt = trace.get("dataVariableType", {}) or {}
        dvt_name = (dvt.get("name") or "").strip().lower()
        dvt_type = (dvt.get("type") or "").strip().lower()

        # Prefer true measured rainfall traces, not generic "Rain" series
        if dvt_type == "rain" and (dvt_name in {"rain", "rainfall"}):
            # Many primary traces are literally "Rainfall" / "Rain"
            return trace_data

        # Some datasets mark primary as type Rain and description contains rainfall
        if dvt_type == "rain" and "rainfall" in description.lower():
            return trace_data

    # 3) Fallback: any visible rain trace that isn't forecast/nowcast etc.
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        if _is_bad_primary_rain_trace(description):
            continue

        dvt = trace.get("dataVariableType", {}) or {}
        dvt_type = (dvt.get("type") or "").strip().lower()

        if dvt_type == "rain" and (trace.get("isVisible") is True):
            return trace_data

    return None


def is_gauge_active(telemetered_time: Optional[datetime], inactive_months: int) -> bool:
    """
    Active = telemeteredMaximumTime within last inactive_months months (relative to NOW).
    """
    if telemetered_time is None:
        return False

    now_dt = now_like(telemetered_time)
    cutoff = months_ago(now_dt, inactive_months)
    return telemetered_time >= cutoff


def filter_gauges(all_data: List[Dict[str, Any]], cfg: FilterConfig) -> Dict[str, Any]:
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

        # Exclude non-Auckland regions by keyword/regex
        if not is_auckland_gauge(gauge_name, exclude_keyword=cfg.exclude_keyword):
            excluded_gauges.append(gauge_data)
            continue

        rainfall_trace_data = get_rainfall_trace(gauge_data.get("traces", []) or [])
        if not rainfall_trace_data:
            logger.warning("No primary Rainfall trace found for gauge: %s (id=%s)", gauge_name, gauge_id)
            no_rainfall_trace.append(gauge_data)
            continue

        rainfall_trace = rainfall_trace_data.get("trace", {}) or {}
        telem_dt = parse_datetime(rainfall_trace.get("telemeteredMaximumTime"))

        if telem_dt is None:
            logger.warning(
                "No/invalid telemeteredMaximumTime for gauge: %s (id=%s) - chosen trace: %s",
                gauge_name,
                gauge_id,
                rainfall_trace.get("description"),
            )
            inactive_gauges.append(gauge_data)
            continue

        if is_gauge_active(telem_dt, cfg.inactive_threshold_months):
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
