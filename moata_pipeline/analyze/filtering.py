"""
Rain Gauge Filtering Module

Provides filtering logic for identifying active Auckland rain gauges with
valid rainfall traces.

Key Functions:
    filter_gauges: Main filtering function
    is_auckland_gauge: Check if gauge is in Auckland region
    get_rainfall_trace: Extract primary rainfall trace from gauge data
    is_gauge_active: Check if gauge has recent data

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Pattern

from moata_pipeline.common.time_utils import months_ago, now_like, parse_datetime


# Version info
__version__ = "1.0.0"


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class FilterConfig:
    """
    Configuration for gauge filtering.
    
    Attributes:
        inactive_threshold_months: Months of inactivity before gauge is considered inactive
        exclude_keyword: Regex pattern to exclude non-Auckland regions
        
    Example:
        >>> config = FilterConfig(
        ...     inactive_threshold_months=6,
        ...     exclude_keyword="northland|waikato|test"
        ... )
    """
    inactive_threshold_months: int = 3
    exclude_keyword: str = "northland|waikato"  # Regex pattern
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.inactive_threshold_months <= 0:
            raise ValueError(
                f"inactive_threshold_months must be positive, got {self.inactive_threshold_months}"
            )
        
        # Validate regex pattern
        try:
            re.compile(self.exclude_keyword)
        except re.error as e:
            raise ValueError(
                f"Invalid exclude_keyword regex pattern: {self.exclude_keyword}\n"
                f"Error: {e}"
            ) from e


# =============================================================================
# Helper Functions
# =============================================================================

def is_auckland_gauge(gauge_name: str, exclude_keyword: str) -> bool:
    """
    Check if gauge is in Auckland region (not excluded by keyword).
    
    Uses case-insensitive regex matching against exclude pattern.
    
    Args:
        gauge_name: Gauge name to check
        exclude_keyword: Regex pattern of regions to exclude
        
    Returns:
        True if gauge is NOT excluded (i.e., is Auckland), False otherwise
        
    Example:
        >>> is_auckland_gauge("Auckland City Gauge", "northland|waikato")
        True
        >>> is_auckland_gauge("Northland Regional Gauge", "northland|waikato")
        False
    """
    name = gauge_name or ""
    
    try:
        # Return True if pattern NOT found (i.e., not excluded)
        return re.search(exclude_keyword, name, flags=re.IGNORECASE) is None
    except re.error:
        # If regex is invalid, log and assume gauge is valid
        logging.getLogger(__name__).warning(
            f"Invalid regex pattern '{exclude_keyword}', assuming gauge is valid"
        )
        return True


def _is_bad_primary_rain_trace(description: str) -> bool:
    """
    Exclude non-primary rainfall traces that can make inactive gauges appear active.
    
    Filters out derived/processed traces like forecasts, nowcasts, merged data, etc.
    
    Args:
        description: Trace description string
        
    Returns:
        True if trace should be excluded, False if it's a primary trace
        
    Example:
        >>> _is_bad_primary_rain_trace("Rainfall")
        False
        >>> _is_bad_primary_rain_trace("Rainfall Forecast")
        True
    """
    desc = (description or "").lower()
    
    bad_tokens = (
        "forecast",
        "nowcast",
        "merged",
        "anomaly",
        "filtered",
        "mirror",
    )
    
    return any(token in desc for token in bad_tokens)


def get_rainfall_trace(traces_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Extract the PRIMARY measured rainfall trace from gauge data.
    
    Uses multi-stage heuristic to find the best rainfall trace:
        1. Exact match on description == "Rainfall"
        2. Data variable type "Rain" with name "Rain" or "Rainfall"
        3. Fallback to any visible rain trace
        
    Args:
        traces_data: List of trace dictionaries from gauge data
        
    Returns:
        Trace data dictionary, or None if no valid rainfall trace found
        
    Example:
        >>> traces = [
        ...     {"trace": {"description": "Rainfall", "isVisible": True}},
        ...     {"trace": {"description": "Rainfall Forecast", "isVisible": True}}
        ... ]
        >>> result = get_rainfall_trace(traces)
        >>> result["trace"]["description"]
        'Rainfall'
    """
    logger = logging.getLogger(__name__)
    
    if not traces_data:
        return None
    
    # Stage 1: Exact match on description == "Rainfall"
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        
        if description == "Rainfall" and not _is_bad_primary_rain_trace(description):
            logger.debug("Found rainfall trace by exact match: 'Rainfall'")
            return trace_data
    
    # Stage 2: Strong heuristic - data variable type check
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        
        if _is_bad_primary_rain_trace(description):
            continue
        
        dvt = trace.get("dataVariableType", {}) or {}
        dvt_name = (dvt.get("name") or "").strip().lower()
        dvt_type = (dvt.get("type") or "").strip().lower()
        
        # Prefer true measured rainfall traces
        if dvt_type == "rain" and dvt_name in {"rain", "rainfall"}:
            logger.debug(f"Found rainfall trace by type/name: {dvt_name}")
            return trace_data
        
        # Some datasets mark primary as type Rain with "rainfall" in description
        if dvt_type == "rain" and "rainfall" in description.lower():
            logger.debug(f"Found rainfall trace by type + description")
            return trace_data
    
    # Stage 3: Fallback - any visible rain trace
    for trace_data in traces_data:
        trace = trace_data.get("trace", {}) or {}
        description = (trace.get("description") or "").strip()
        
        if _is_bad_primary_rain_trace(description):
            continue
        
        dvt = trace.get("dataVariableType", {}) or {}
        dvt_type = (dvt.get("type") or "").strip().lower()
        
        if dvt_type == "rain" and trace.get("isVisible") is True:
            logger.debug(f"Found rainfall trace by fallback: visible rain type")
            return trace_data
    
    logger.debug("No valid rainfall trace found")
    return None


def is_gauge_active(
    telemetered_time: Optional[datetime],
    inactive_months: int
) -> bool:
    """
    Check if gauge is active based on last telemetry time.
    
    A gauge is considered active if its last telemetry time is within
    the specified number of months from now.
    
    Args:
        telemetered_time: Last telemetry timestamp
        inactive_months: Inactivity threshold in months
        
    Returns:
        True if gauge is active, False otherwise
        
    Example:
        >>> from datetime import datetime, timezone, timedelta
        >>> recent = datetime.now(timezone.utc) - timedelta(days=30)
        >>> is_gauge_active(recent, inactive_months=3)
        True
        >>> old = datetime.now(timezone.utc) - timedelta(days=200)
        >>> is_gauge_active(old, inactive_months=3)
        False
    """
    if telemetered_time is None:
        return False
    
    if inactive_months <= 0:
        raise ValueError(
            f"inactive_months must be positive, got {inactive_months}"
        )
    
    now_dt = now_like(telemetered_time)
    cutoff = months_ago(now_dt, inactive_months)
    
    return telemetered_time >= cutoff


# =============================================================================
# Main Filtering Function
# =============================================================================

def filter_gauges(
    all_data: List[Dict[str, Any]],
    cfg: FilterConfig
) -> Dict[str, Any]:
    """
    Filter gauges to identify active Auckland rain gauges.
    
    Filtering Logic:
        1. Exclude non-Auckland regions by keyword/regex
        2. Find primary rainfall trace for each gauge
        3. Check if gauge has recent data (within inactive_months)
        4. Categorize gauges as active, inactive, excluded, or no_rainfall_trace
        
    Args:
        all_data: List of gauge data dictionaries
        cfg: Filtering configuration
        
    Returns:
        Dictionary containing:
            - active_gauges: List of active Auckland gauges
            - inactive_gauges: List of inactive gauges
            - excluded_gauges: List of excluded (non-Auckland) gauges
            - no_rainfall_trace: List of gauges without valid rainfall trace
            - stats: Summary statistics dictionary
            
    Raises:
        ValueError: If all_data is not a list
        
    Example:
        >>> config = FilterConfig(inactive_threshold_months=3)
        >>> result = filter_gauges(all_data, config)
        >>> print(f"Found {len(result['active_gauges'])} active gauges")
        >>> print(f"Stats: {result['stats']}")
    """
    logger = logging.getLogger(__name__)
    
    # Validate input
    if not isinstance(all_data, list):
        raise ValueError(
            f"all_data must be a list, got {type(all_data).__name__}"
        )
    
    # Initialize result lists
    active_gauges: List[Dict[str, Any]] = []
    inactive_gauges: List[Dict[str, Any]] = []
    excluded_gauges: List[Dict[str, Any]] = []
    no_rainfall_trace: List[Dict[str, Any]] = []
    
    total = len(all_data)
    logger.info(f"Processing {total} gauges...")
    logger.info(f"  Inactive threshold: {cfg.inactive_threshold_months} months")
    logger.info(f"  Exclude pattern: '{cfg.exclude_keyword}'")
    
    # Process each gauge
    for idx, gauge_data in enumerate(all_data, start=1):
        gauge = gauge_data.get("gauge", {}) or {}
        gauge_name = gauge.get("name", "Unknown")
        gauge_id = gauge.get("id")
        
        logger.debug(f"[{idx}/{total}] Processing: {gauge_name}")
        
        # Step 1: Check if gauge is in Auckland region
        if not is_auckland_gauge(gauge_name, exclude_keyword=cfg.exclude_keyword):
            logger.debug(f"  Excluded (non-Auckland): {gauge_name}")
            excluded_gauges.append(gauge_data)
            continue
        
        # Step 2: Find primary rainfall trace
        rainfall_trace_data = get_rainfall_trace(gauge_data.get("traces", []) or [])
        
        if not rainfall_trace_data:
            logger.warning(
                f"  No primary rainfall trace found: {gauge_name} (id={gauge_id})"
            )
            no_rainfall_trace.append(gauge_data)
            continue
        
        rainfall_trace = rainfall_trace_data.get("trace", {}) or {}
        
        # Step 3: Check telemetry time
        telem_time_str = rainfall_trace.get("telemeteredMaximumTime")
        telem_dt = parse_datetime(telem_time_str)
        
        if telem_dt is None:
            logger.warning(
                f"  No/invalid telemeteredMaximumTime: {gauge_name} (id={gauge_id}), "
                f"trace: {rainfall_trace.get('description')}"
            )
            inactive_gauges.append(gauge_data)
            continue
        
        # Step 4: Check if gauge is active
        if is_gauge_active(telem_dt, cfg.inactive_threshold_months):
            # Enrich gauge data with additional info
            gauge_data["last_data_time"] = telem_dt.isoformat()
            gauge_data["last_data_time_dt"] = telem_dt
            gauge_data["rainfall_trace"] = rainfall_trace
            
            active_gauges.append(gauge_data)
            
            logger.info(
                f"  ✓ Active: {gauge_name[:60]} (last: {telem_dt.strftime('%Y-%m-%d')})"
            )
        else:
            logger.debug(
                f"  Inactive: {gauge_name} (last: {telem_dt.strftime('%Y-%m-%d')})"
            )
            inactive_gauges.append(gauge_data)
    
    # Generate statistics
    stats = {
        "total_gauges": total,
        "active_auckland_gauges": len(active_gauges),
        "inactive_gauges": len(inactive_gauges),
        "excluded_gauges": len(excluded_gauges),
        "no_rainfall_trace": len(no_rainfall_trace),
        "num_excluded": len(excluded_gauges),
        "num_inactive": len(inactive_gauges),
    }
    
    logger.info("")
    logger.info("Filtering complete:")
    logger.info(f"  Total gauges: {stats['total_gauges']}")
    logger.info(f"  Active (Auckland): {stats['active_auckland_gauges']}")
    logger.info(f"  Inactive: {stats['inactive_gauges']}")
    logger.info(f"  Excluded (non-Auckland): {stats['excluded_gauges']}")
    logger.info(f"  No rainfall trace: {stats['no_rainfall_trace']}")
    
    return {
        "active_gauges": active_gauges,
        "inactive_gauges": inactive_gauges,
        "excluded_gauges": excluded_gauges,
        "no_rainfall_trace": no_rainfall_trace,
        "stats": stats,
    }