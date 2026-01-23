"""
Moata API Client Module (Facade Pattern)

Unified client that delegates to specialized clients for different API domains.
This maintains backwards compatibility while promoting Single Responsibility Principle.

**New Architecture (SOLID Compliant):**
- AssetClient: Asset operations (rain gauges, catchments)
- TraceClient: Trace/timeseries operations
- RadarClient: Radar/QPE operations  
- AlarmClient: Alarm and threshold operations
- ARIClient: ARI (Average Recurrence Interval) operations

**Backwards Compatibility:**
The MoataClient class provides the same interface as before, but now delegates
to specialized clients internally. This allows gradual migration.

Usage:
    from moata_pipeline.moata.client import MoataClient
    from moata_pipeline.moata.http import MoataHttp
    from moata_pipeline.moata.auth import MoataAuth
    
    # Initialize (same as before)
    auth = MoataAuth(...)
    http = MoataHttp(get_token_fn=auth.get_token, ...)
    client = MoataClient(http=http)
    
    # Old style (still works - backwards compatible)
    gauges = client.get_rain_gauges(project_id=594, asset_type_id=100)
    
    # New style (recommended - uses specialized clients)
    gauges = client.assets.get_rain_gauges(project_id=594, asset_type_id=100)
    traces = client.traces.get_traces_for_asset(asset_id=12345)
    pixels = client.radar.get_pixel_mappings_for_geometry(...)
    alarms = client.alarms.get_alarms_for_trace(trace_id=12345)
    ari = client.ari.get_ari_data(trace_id=12345, from_time=..., to_time=...)

API Documentation:
    Full API docs available at Moata API documentation portal.
    
Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21 (Refactored for SOLID compliance)
"""

import logging
from typing import Any, Dict, List, Optional, Union

from .http import MoataHttp
from .clients import (
    AssetClient,
    TraceClient,
    RadarClient,
    AlarmClient,
    ARIClient,
    ValidationError,
)

# Re-export for backwards compatibility
__all__ = ["MoataClient", "ValidationError"]


class MoataClient:
    """
    Unified Moata API client (Facade Pattern).
    
    Delegates to specialized clients for different API areas:
    - **assets**: AssetClient for asset operations
    - **traces**: TraceClient for trace/timeseries operations
    - **radar**: RadarClient for radar/QPE operations
    - **alarms**: AlarmClient for alarm operations
    - **ari**: ARIClient for ARI operations
    
    **Why this change?**
    - Single Responsibility: Each client handles one domain
    - Open/Closed: Easy to add new clients without modifying existing code
    - Interface Segregation: Clients only expose relevant methods
    - Testability: Mock individual clients independently
    
    **Backwards Compatibility:**
    All old methods still work via delegation. You can migrate gradually:
    - `client.get_rain_gauges(...)` → `client.assets.get_rain_gauges(...)`
    - `client.get_trace_data(...)` → `client.traces.get_trace_data(...)`
    
    Example:
        >>> # Initialize
        >>> client = MoataClient(http=http_client)
        >>> 
        >>> # New style (recommended)
        >>> gauges = client.assets.get_rain_gauges(594, 25)
        >>> traces = client.traces.get_traces_for_asset(12345)
        >>> 
        >>> # Old style (still works)
        >>> gauges = client.get_rain_gauges(594, 25)
        >>> traces = client.get_traces_for_asset(12345)
    """
    
    def __init__(self, http: MoataHttp) -> None:
        """
        Initialize unified client with specialized sub-clients.
        
        Args:
            http: Configured MoataHttp instance
            
        Raises:
            ValueError: If http is None
        """
        if http is None:
            raise ValueError("http cannot be None")
        
        # Initialize specialized clients
        self.assets = AssetClient(http)
        self.traces = TraceClient(http)
        self.radar = RadarClient(http)
        self.alarms = AlarmClient(http)
        self.ari = ARIClient(http)
        
        # Store for backwards compatibility
        self._http = http
        self._logger = logging.getLogger(__name__)
        
        self._logger.debug("MoataClient initialized with specialized clients")
    
    # ========================================================================
    # Backwards Compatibility Methods - Asset Operations
    # ========================================================================
    
    def get_rain_gauges(
        self,
        project_id: Union[int, str],
        asset_type_id: Union[int, str],
        exclude_keyword: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to assets.get_rain_gauges()."""
        return self.assets.get_rain_gauges(project_id, asset_type_id, exclude_keyword)
    
    def get_assets_with_geometry(
        self,
        project_id: Union[int, str],
        asset_type_id: Union[int, str],
        exclude_keyword: Optional[str] = None,
        sr_id: int = 4326
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to assets.get_assets_with_geometry()."""
        return self.assets.get_assets_with_geometry(
            project_id, asset_type_id, exclude_keyword, sr_id
        )
    
    def get_asset_by_id(self, asset_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Backwards compatible: delegates to assets.get_asset_by_id()."""
        return self.assets.get_asset_by_id(asset_id)
    
    def get_asset_names_batch(
        self,
        asset_ids: List[Union[int, str]]
    ) -> Dict[int, str]:
        """Backwards compatible: delegates to assets.get_asset_names_batch()."""
        return self.assets.get_asset_names_batch(asset_ids)
    
    # ========================================================================
    # Backwards Compatibility Methods - Trace Operations
    # ========================================================================
    
    def get_traces_for_asset(self, asset_id: Union[int, str]) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to traces.get_traces_for_asset()."""
        return self.traces.get_traces_for_asset(asset_id)
    
    def get_traces_for_assets(
        self,
        asset_ids: List[Union[int, str]],
        max_workers: int = 10
    ) -> Dict[int, List[Dict[str, Any]]]:
        """Backwards compatible: delegates to traces.get_traces_for_assets()."""
        return self.traces.get_traces_for_assets(asset_ids, max_workers)
    
    def get_trace_data(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        data_type: str = "None",
        data_interval: int = 300,
        pad_with_zeroes: bool = False
    ) -> Dict[str, Any]:
        """Backwards compatible: delegates to traces.get_trace_data()."""
        return self.traces.get_trace_data(
            trace_id, from_time, to_time, data_type, data_interval, pad_with_zeroes
        )
    
    def get_trace_data_as_list(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        pad_with_zeroes: bool = False
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to traces.get_trace_data_as_list()."""
        return self.traces.get_trace_data_as_list(
            trace_id, from_time, to_time, pad_with_zeroes
        )
    
    # ========================================================================
    # Backwards Compatibility Methods - Radar Operations
    # ========================================================================
    
    def get_pixel_mappings_for_geometry(
        self,
        traceset_id: Union[int, str],
        geometry: Dict[str, Any],
        sr_id: int = 4326
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to radar.get_pixel_mappings_for_geometry()."""
        return self.radar.get_pixel_mappings_for_geometry(traceset_id, geometry, sr_id)
    
    def get_traceset_data(
        self,
        traceset_id: Union[int, str],
        pixel_ids: List[int],
        from_time: str,
        to_time: str
    ) -> Dict[str, Any]:
        """Backwards compatible: delegates to radar.get_traceset_data()."""
        return self.radar.get_traceset_data(traceset_id, pixel_ids, from_time, to_time)
    
    def get_traceset_data_batched(
        self,
        traceset_id: Union[int, str],
        pixel_ids: List[int],
        from_time: str,
        to_time: str,
        batch_size: int = 50
    ) -> Dict[str, Any]:
        """Backwards compatible: delegates to radar.get_traceset_data_batched()."""
        return self.radar.get_traceset_data_batched(
            traceset_id, pixel_ids, from_time, to_time, batch_size
        )
    
    def get_pixel_mappings_metadata(
        self,
        traceset_id: Union[int, str],
        pixel_ids: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to radar.get_pixel_mappings_metadata()."""
        return self.radar.get_pixel_mappings_metadata(traceset_id, pixel_ids)
    
    # ========================================================================
    # Backwards Compatibility Methods - Alarm Operations
    # ========================================================================
    
    def get_alarms_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to alarms.get_alarms_for_trace()."""
        return self.alarms.get_alarms_for_trace(trace_id)
    
    def get_overflow_alarms_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to alarms.get_overflow_alarms_for_trace()."""
        return self.alarms.get_overflow_alarms_for_trace(trace_id)
    
    def get_recency_alarms_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to alarms.get_recency_alarms_for_trace()."""
        return self.alarms.get_recency_alarms_for_trace(trace_id)
    
    def split_alarms_by_type(
        self,
        alarms: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Backwards compatible: delegates to alarms.split_alarms_by_type()."""
        return self.alarms.split_alarms_by_type(alarms)
    
    def get_thresholds_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """Backwards compatible: delegates to alarms.get_thresholds_for_trace()."""
        return self.alarms.get_thresholds_for_trace(trace_id)
    
    def get_detailed_alarms_by_project(
        self,
        project_id: int
    ) -> Dict[int, Dict[str, Any]]:
        """Backwards compatible: delegates to alarms.get_detailed_alarms_by_project()."""
        return self.alarms.get_detailed_alarms_by_project(project_id)
    
    # ========================================================================
    # Backwards Compatibility Methods - ARI Operations
    # ========================================================================
    
    def get_ari_data(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        ari_type: str = "Tp108",
    ) -> Any:
        """Backwards compatible: delegates to ari.get_ari_data()."""
        return self.ari.get_ari_data(trace_id, from_time, to_time, ari_type)
