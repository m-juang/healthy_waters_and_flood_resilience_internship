"""
Rainfall Trace Filter - Pre-collection filtering for active gauges.

This module implements Sam's optimization suggestion: use the /projects/{id}/traces/info
endpoint to quickly identify active rainfall traces and filter out inactive gauges
BEFORE fetching all traces/alarms, reducing API calls significantly.

SOLID Principles:
    - Single Responsibility: Only handles rainfall trace filtering
    - Open/Closed: Configurable via FilterCriteria dataclass
    - Dependency Inversion: Depends on protocol, not concrete client

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: January 2026
Version: 1.0.0
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol, Set

from moata_pipeline.common.time_utils import months_ago, parse_datetime


__version__ = "1.0.0"


# =============================================================================
# Protocols (Dependency Inversion)
# =============================================================================

class TracesInfoProvider(Protocol):
    """Protocol for fetching project traces info."""
    
    def get_project_traces_info(
        self,
        project_id: int,
        data_variable_type_id: Optional[int] = None,
        description: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch trace info for a project with optional filters."""
        ...


# =============================================================================
# Configuration (Open/Closed)
# =============================================================================

@dataclass(frozen=True)
class FilterCriteria:
    """
    Configuration for rainfall trace filtering.
    
    Attributes:
        data_variable_type_id: Filter by data variable type (10 = rainfall)
        description: Filter by trace description
        inactive_months: Months threshold for inactive gauges
        exclude_patterns: Regex patterns to exclude from gauge names
        
    Example:
        >>> criteria = FilterCriteria(
        ...     data_variable_type_id=10,
        ...     description="Rainfall",
        ...     inactive_months=3
        ... )
    """
    data_variable_type_id: int = 10  # Rainfall data variable type
    description: str = "Rainfall"
    inactive_months: int = 3
    exclude_patterns: tuple = ("northland", "waikato")
    
    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.inactive_months <= 0:
            raise ValueError(f"inactive_months must be positive, got {self.inactive_months}")


@dataclass
class FilterResult:
    """
    Result of rainfall trace filtering.
    
    Attributes:
        active_asset_ids: Set of asset IDs with active rainfall data
        inactive_asset_ids: Set of asset IDs with stale rainfall data
        excluded_asset_ids: Set of asset IDs excluded by name pattern
        trace_info_by_asset: Mapping of asset_id -> trace info dict
        total_traces: Total traces processed
    """
    active_asset_ids: Set[int] = field(default_factory=set)
    inactive_asset_ids: Set[int] = field(default_factory=set)
    excluded_asset_ids: Set[int] = field(default_factory=set)
    trace_info_by_asset: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    total_traces: int = 0
    
    @property
    def active_count(self) -> int:
        """Number of active assets."""
        return len(self.active_asset_ids)
    
    @property
    def inactive_count(self) -> int:
        """Number of inactive assets."""
        return len(self.inactive_asset_ids)
    
    @property
    def excluded_count(self) -> int:
        """Number of excluded assets."""
        return len(self.excluded_asset_ids)


# =============================================================================
# Main Filter Class (Single Responsibility)
# =============================================================================

class RainfallTraceFilter:
    """
    Filter for identifying active rain gauges using project traces info endpoint.
    
    This implements Sam's optimization: instead of fetching traces for each asset
    individually, use the bulk traces/info endpoint to quickly identify which
    gauges are active, then only fetch full data for active gauges.
    
    Single Responsibility:
        - Filter rainfall traces to identify active/inactive gauges
        
    Usage:
        >>> filter = RainfallTraceFilter(client, FilterCriteria())
        >>> result = filter.filter_active_gauges(project_id=594)
        >>> print(f"Active: {result.active_count}, Inactive: {result.inactive_count}")
    """
    
    def __init__(
        self,
        client: TracesInfoProvider,
        criteria: Optional[FilterCriteria] = None
    ) -> None:
        """
        Initialize rainfall trace filter.
        
        Args:
            client: Client that provides get_project_traces_info method
            criteria: Filtering criteria (uses defaults if None)
        """
        self._client = client
        self._criteria = criteria or FilterCriteria()
        self._logger = logging.getLogger(f"{__name__}.RainfallTraceFilter")
    
    def filter_active_gauges(
        self,
        project_id: int,
        asset_names: Optional[Dict[int, str]] = None
    ) -> FilterResult:
        """
        Filter to identify active rain gauges.
        
        Args:
            project_id: Moata project ID
            asset_names: Optional dict mapping asset_id -> asset_name for exclusion
            
        Returns:
            FilterResult with active/inactive/excluded asset sets
            
        Example:
            >>> result = filter.filter_active_gauges(594)
            >>> active_ids = result.active_asset_ids
        """
        self._logger.info("=" * 60)
        self._logger.info("Rainfall Trace Pre-Filter (Optimization)")
        self._logger.info("=" * 60)
        self._logger.info(f"Project ID: {project_id}")
        self._logger.info(f"Data Variable Type ID: {self._criteria.data_variable_type_id}")
        self._logger.info(f"Description: '{self._criteria.description}'")
        self._logger.info(f"Inactive threshold: {self._criteria.inactive_months} months")
        
        result = FilterResult()
        
        # Fetch rainfall traces info from bulk endpoint
        self._logger.info("Fetching rainfall traces info...")
        traces_info = self._client.get_project_traces_info(
            project_id=project_id,
            data_variable_type_id=self._criteria.data_variable_type_id,
            description=self._criteria.description,
        )
        
        result.total_traces = len(traces_info)
        self._logger.info(f"✓ Retrieved {result.total_traces} rainfall traces")
        
        if not traces_info:
            self._logger.warning("No rainfall traces found")
            return result
        
        # Calculate cutoff date
        now = datetime.now(timezone.utc)
        cutoff = months_ago(now, self._criteria.inactive_months)
        self._logger.info(f"Activity cutoff: {cutoff.strftime('%Y-%m-%d')}")
        
        # Process each trace
        for trace_info in traces_info:
            self._process_trace(trace_info, cutoff, asset_names, result)
        
        # Log summary
        self._log_summary(result)
        
        return result
    
    def _process_trace(
        self,
        trace_info: Dict[str, Any],
        cutoff: datetime,
        asset_names: Optional[Dict[int, str]],
        result: FilterResult
    ) -> None:
        """
        Process a single trace info record.
        
        Args:
            trace_info: Trace info dictionary from API
            cutoff: Cutoff datetime for activity check
            asset_names: Optional asset name mapping
            result: FilterResult to update
        """
        asset_id = trace_info.get("assetId")
        if not asset_id:
            return
        
        asset_id = int(asset_id)
        
        # Check exclusion by name
        if asset_names:
            asset_name = asset_names.get(asset_id, "")
            if self._should_exclude(asset_name):
                result.excluded_asset_ids.add(asset_id)
                return
        
        # Store trace info
        result.trace_info_by_asset[asset_id] = trace_info
        
        # Check activity using telemeteredMaximumTime
        telemetered_time = self._parse_telemetered_time(trace_info)
        
        if telemetered_time and telemetered_time >= cutoff:
            result.active_asset_ids.add(asset_id)
        else:
            result.inactive_asset_ids.add(asset_id)
    
    def _should_exclude(self, asset_name: str) -> bool:
        """
        Check if asset should be excluded by name pattern.
        
        Args:
            asset_name: Asset name to check
            
        Returns:
            True if should be excluded
        """
        name_lower = asset_name.lower()
        return any(pattern in name_lower for pattern in self._criteria.exclude_patterns)
    
    def _parse_telemetered_time(self, trace_info: Dict[str, Any]) -> Optional[datetime]:
        """
        Parse telemeteredMaximumTime from trace info.
        
        Args:
            trace_info: Trace info dictionary
            
        Returns:
            Parsed datetime or None
        """
        time_str = trace_info.get("telemeteredMaximumTime")
        if not time_str:
            return None
        
        try:
            return parse_datetime(time_str)
        except (ValueError, TypeError):
            self._logger.debug(f"Failed to parse telemeteredMaximumTime: {time_str}")
            return None
    
    def _log_summary(self, result: FilterResult) -> None:
        """Log filtering summary."""
        self._logger.info("")
        self._logger.info("Filter Results:")
        self._logger.info(f"  ✓ Active gauges: {result.active_count}")
        self._logger.info(f"  ✗ Inactive gauges: {result.inactive_count}")
        if result.excluded_count > 0:
            self._logger.info(f"  ⊘ Excluded (non-Auckland): {result.excluded_count}")
        self._logger.info(f"  Total traces processed: {result.total_traces}")
        self._logger.info("=" * 60)
