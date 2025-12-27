"""
Moata API Client Module

High-level client for the Moata API providing domain-specific methods for:
- Asset management (rain gauges, catchments)
- Trace data (timeseries)
- Radar data (TraceSet collections)
- Alarms and thresholds
- ARI (Annual Recurrence Interval) data

Usage:
    from moata_pipeline.moata.client import MoataClient
    from moata_pipeline.moata.http import MoataHttp
    from moata_pipeline.moata.auth import MoataAuth
    
    # Initialize
    auth = MoataAuth(...)
    http = MoataHttp(get_token_fn=auth.get_token, ...)
    client = MoataClient(http=http)
    
    # Get rain gauges
    gauges = client.get_rain_gauges(project_id=594, asset_type_id=25)
    
    # Get trace data
    data = client.get_trace_data(
        trace_id=12345,
        from_time="2025-01-01T00:00:00Z",
        to_time="2025-01-31T23:59:59Z"
    )

API Documentation:
    Full API docs available at Moata API documentation portal.
    
Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import logging
from typing import Any, Dict, List, Optional, Union

from .http import MoataHttp
from . import endpoints as ep

# Constants
DEFAULT_SR_ID = 4326  # WGS84
DEFAULT_DATA_TYPE = "None"
DEFAULT_RADAR_BATCH_SIZE = 50
MAX_RADAR_BATCH_SIZE = 150
DEFAULT_PAD_WITH_ZEROES = False


class ValidationError(Exception):
    """Raised when parameter validation fails."""
    pass


class MoataClient:
    """
    High-level client for Moata API.
    
    Provides domain-specific methods organized by functionality:
    - Assets (rain gauges, catchments, etc.)
    - Traces (timeseries metadata)
    - Trace Data (timeseries values)
    - Radar Data (TraceSet collections, QPE)
    - Alarms (overflow, recency)
    - Thresholds
    - ARI (Annual Recurrence Interval)
    
    Attributes:
        _http: HTTP client for making requests
        _logger: Logger instance
        
    Example:
        >>> client = MoataClient(http=http_client)
        >>> gauges = client.get_rain_gauges(594, 25)
        >>> len(gauges)
        200
    """
    
    def __init__(self, http: MoataHttp) -> None:
        """
        Initialize Moata API client.
        
        Args:
            http: Configured MoataHttp instance
            
        Raises:
            ValueError: If http is None
        """
        if http is None:
            raise ValueError("http cannot be None")
        
        self._http = http
        self._logger = logging.getLogger(__name__)
        
        self._logger.debug("MoataClient initialized")

    # ========================================================================
    # ASSETS
    # ========================================================================
    
    def get_rain_gauges(
        self,
        project_id: int,
        asset_type_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get rain gauge assets for a project.
        
        GET /v1/projects/{projectId}/assets?assetTypeId=...
        
        Args:
            project_id: Moata project ID (e.g., 594 for Auckland Council)
            asset_type_id: Asset type ID for rain gauges (e.g., 25)
            
        Returns:
            List of AssetWithGeometryDto dictionaries
            
        Example:
            >>> gauges = client.get_rain_gauges(594, 25)
            >>> gauge = gauges[0]
            >>> print(gauge['assetName'], gauge['assetId'])
        """
        self._validate_id(project_id, "project_id")
        self._validate_id(asset_type_id, "asset_type_id")
        
        path = ep.PROJECT_ASSETS.format(project_id=int(project_id))
        data = self._http.get(path, params={"assetTypeId": int(asset_type_id)})
        
        return self._extract_items(data)

    def get_assets_with_geometry(
        self,
        project_id: int,
        asset_type_id: Optional[int] = None,
        sr_id: int = DEFAULT_SR_ID,
        asset_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get assets with geometry for a project.
        
        GET /v1/projects/{projectId}/assets
        
        Args:
            project_id: Moata project ID
            asset_type_id: Optional asset type filter (e.g., 3541 for catchments)
            sr_id: Spatial Reference ID for geometry (default: 4326 = WGS84)
            asset_name: Optional filter by asset name
            
        Returns:
            List of AssetWithGeometryDto with geometryWkt field
            
        Example:
            >>> catchments = client.get_assets_with_geometry(594, asset_type_id=3541)
            >>> catchment = catchments[0]
            >>> print(catchment['assetName'], catchment['geometryWkt'])
        """
        self._validate_id(project_id, "project_id")
        if asset_type_id is not None:
            self._validate_id(asset_type_id, "asset_type_id")
        
        path = ep.PROJECT_ASSETS.format(project_id=int(project_id))
        params: Dict[str, Any] = {"srId": sr_id}
        
        if asset_type_id is not None:
            params["assetTypeId"] = int(asset_type_id)
        if asset_name is not None:
            params["assetName"] = asset_name
        
        data = self._http.get(path, params=params)
        return self._extract_items(data)

    # ========================================================================
    # TRACES
    # ========================================================================
    
    def get_traces_for_asset(self, asset_id: Union[int, str]) -> List[Dict[str, Any]]:
        """
        Get all traces (timeseries) for a single asset.
        
        GET /v1/assets/traces?assetId=<id>
        
        Args:
            asset_id: Asset ID (int or convertible to int)
            
        Returns:
            List of TraceDto dictionaries
            
        Example:
            >>> traces = client.get_traces_for_asset(12345)
            >>> for trace in traces:
            ...     print(trace['traceId'], trace['traceDescription'])
        """
        asset_id_int = self._validate_id(asset_id, "asset_id")
        
        params = {"assetId": [asset_id_int]}
        data = self._http.get(ep.ASSET_TRACES, params=params)
        
        return self._extract_items(data)

    def get_traces_for_assets(
        self,
        asset_ids: List[Union[int, str]],
        data_variable_type_id: Optional[int] = None,
        scenario_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get traces for multiple assets (batch request).
        
        GET /v1/assets/traces?assetId=1&assetId=2...
        
        Args:
            asset_ids: List of asset IDs
            data_variable_type_id: Optional filter by data variable type
            scenario_id: Optional filter by scenario
            
        Returns:
            List of TraceDto dictionaries for all assets
            
        Example:
            >>> traces = client.get_traces_for_assets([12345, 12346, 12347])
            >>> print(f"Found {len(traces)} traces across {len(asset_ids)} assets")
        """
        if not asset_ids:
            raise ValidationError("asset_ids cannot be empty")
        
        asset_ids_int = [self._validate_id(aid, f"asset_ids[{i}]") 
                         for i, aid in enumerate(asset_ids)]
        
        params: Dict[str, Any] = {"assetId": asset_ids_int}
        
        if data_variable_type_id is not None:
            params["dataVariableTypeId"] = int(data_variable_type_id)
        if scenario_id is not None:
            params["scenarioId"] = int(scenario_id)
        
        data = self._http.get(ep.ASSET_TRACES, params=params)
        return self._extract_items(data)

    # ========================================================================
    # TRACE DATA (TIMESERIES)
    # ========================================================================
    
    def get_trace_data(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        data_type: str = DEFAULT_DATA_TYPE,
        data_interval: Optional[int] = None,
        pad_with_zeroes: bool = DEFAULT_PAD_WITH_ZEROES,
    ) -> Dict[str, Any]:
        """
        Get timeseries data for a trace.
        
        GET /v1/traces/{traceId}/data/utc
        
        Args:
            trace_id: Trace ID (int or convertible to int)
            from_time: Start time (ISO 8601, e.g., "2025-01-01T00:00:00Z")
            to_time: End time (ISO 8601, e.g., "2025-01-31T23:59:59Z")
            data_type: Data type (default: "None" for raw data)
            data_interval: Optional data interval in seconds
            pad_with_zeroes: Whether to pad missing values with zeros
            
        Returns:
            Dictionary with:
            - items: List of data points [{time, value}, ...]
            - pageNumber, itemsPerPage, totalItems
            
        Example:
            >>> data = client.get_trace_data(
            ...     trace_id=12345,
            ...     from_time="2025-01-01T00:00:00Z",
            ...     to_time="2025-01-07T23:59:59Z"
            ... )
            >>> print(f"Retrieved {len(data['items'])} data points")
        """
        trace_id_int = self._validate_id(trace_id, "trace_id")
        self._validate_time_string(from_time, "from_time")
        self._validate_time_string(to_time, "to_time")
        
        path = ep.TRACE_DATA_UTC.format(trace_id=trace_id_int)
        
        params: Dict[str, Any] = {
            "from": from_time,
            "to": to_time,
            "dataType": data_type,
            "padWithZeroes": str(pad_with_zeroes).lower(),
        }
        
        if data_interval is not None:
            if data_interval <= 0:
                raise ValidationError(f"data_interval must be positive, got {data_interval}")
            params["dataInterval"] = int(data_interval)
        
        data = self._http.get(path, params=params, allow_404=True)
        
        # Return empty structure if no data
        if data is None:
            return {
                "items": [],
                "pageNumber": 0,
                "itemsPerPage": 0,
                "totalItems": 0
            }
        
        # Normalize response
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return {"items": data}
        
        return {"items": []}

    def get_trace_data_as_list(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        data_type: str = DEFAULT_DATA_TYPE,
        data_interval: Optional[int] = None,
        pad_with_zeroes: bool = DEFAULT_PAD_WITH_ZEROES,
    ) -> List[Dict[str, Any]]:
        """
        Get timeseries data as a list (convenience wrapper).
        
        Same as get_trace_data but returns only the items list.
        
        Args:
            (Same as get_trace_data)
            
        Returns:
            List of data points [{time, value}, ...]
            
        Example:
            >>> items = client.get_trace_data_as_list(
            ...     trace_id=12345,
            ...     from_time="2025-01-01T00:00:00Z",
            ...     to_time="2025-01-07T23:59:59Z"
            ... )
            >>> for item in items:
            ...     print(item['time'], item['value'])
        """
        result = self.get_trace_data(
            trace_id=trace_id,
            from_time=from_time,
            to_time=to_time,
            data_type=data_type,
            data_interval=data_interval,
            pad_with_zeroes=pad_with_zeroes,
        )
        return result.get("items", [])

    # ========================================================================
    # RADAR / TRACESET COLLECTIONS
    # ========================================================================
    
    def get_pixel_mappings_for_geometry(
        self,
        collection_id: int,
        wkt: str,
        sr_id: int = DEFAULT_SR_ID,
    ) -> List[Dict[str, Any]]:
        """
        Get radar pixel indices that intersect with a geometry.
        
        GET /v1/trace-set-collections/{id}/pixel-mappings/intersects-geometry
        
        Args:
            collection_id: TraceSet collection ID (e.g., 1 for radar QPE)
            wkt: Well-Known Text geometry string (e.g., "POLYGON((...))") 
            sr_id: Spatial Reference ID (default: 4326 = WGS84)
            
        Returns:
            List of TraceSetPixelMappingDto:
            [{"pixelIndex": int, "geometryWkt": str}, ...]
            
        Example:
            >>> pixels = client.get_pixel_mappings_for_geometry(
            ...     collection_id=1,
            ...     wkt="POLYGON((174.0 -37.0, 174.1 -37.0, ...))"
            ... )
            >>> pixel_indices = [p['pixelIndex'] for p in pixels]
        """
        self._validate_id(collection_id, "collection_id")
        if not wkt or not wkt.strip():
            raise ValidationError("wkt cannot be empty")
        
        path = ep.TRACESET_PIXEL_MAPPINGS.format(collection_id=int(collection_id))
        params = {"wkt": wkt, "srId": sr_id}
        
        data = self._http.get(path, params=params, allow_404=True)
        
        if data is None:
            return []
        
        return self._extract_items(data)

    def get_traceset_data(
        self,
        collection_id: int,
        traceset_ids: List[int],
        pixel_indices: List[int],
        start_time: str,
        end_time: str,
    ) -> List[Dict[str, Any]]:
        """
        Get radar data values for specified tracesets and pixels.
        
        GET /v1/trace-set-collections/{id}/trace-sets/data
        
        Args:
            collection_id: TraceSet collection ID (e.g., 1 for radar QPE)
            traceset_ids: List of traceset IDs (e.g., [3] for QPE)
            pixel_indices: List of pixel indices (max 150 per request)
            start_time: Start time (ISO 8601, e.g., "2025-05-01T00:00:00Z")
            end_time: End time (ISO 8601, e.g., "2025-05-01T23:59:59Z")
            
        Returns:
            List of TraceSetDataValuesDto:
            [{
                "traceSetId": int,
                "pixelIndex": int,
                "startTime": str,
                "endTime": str,
                "values": [float, ...]
            }, ...]
            
        Raises:
            ValidationError: If pixel_indices exceeds maximum
            
        Notes:
            - Limit: max 150 pixels per request
            - For >150 pixels, use get_traceset_data_batched()
            - Recommended: max 24 hours of data per request
            
        Example:
            >>> data = client.get_traceset_data(
            ...     collection_id=1,
            ...     traceset_ids=[3],
            ...     pixel_indices=[100, 101, 102],
            ...     start_time="2025-05-01T00:00:00Z",
            ...     end_time="2025-05-01T23:59:59Z"
            ... )
        """
        self._validate_id(collection_id, "collection_id")
        
        if not traceset_ids:
            raise ValidationError("traceset_ids cannot be empty")
        if not pixel_indices:
            raise ValidationError("pixel_indices cannot be empty")
        if len(pixel_indices) > MAX_RADAR_BATCH_SIZE:
            raise ValidationError(
                f"pixel_indices exceeds maximum of {MAX_RADAR_BATCH_SIZE}. "
                f"Use get_traceset_data_batched() for larger requests."
            )
        
        self._validate_time_string(start_time, "start_time")
        self._validate_time_string(end_time, "end_time")
        
        path = ep.TRACESET_COLLECTION_DATA.format(collection_id=int(collection_id))
        params = {
            "TsId": [int(x) for x in traceset_ids],
            "Pi": [int(x) for x in pixel_indices],
            "StartTime": start_time,
            "EndTime": end_time,
        }
        
        data = self._http.get(path, params=params, allow_404=True)
        
        if data is None:
            return []
        
        return self._extract_items(data)

    def get_traceset_data_batched(
        self,
        collection_id: int,
        traceset_ids: List[int],
        pixel_indices: List[int],
        start_time: str,
        end_time: str,
        batch_size: int = DEFAULT_RADAR_BATCH_SIZE,
    ) -> List[Dict[str, Any]]:
        """
        Get radar data with automatic batching for large pixel lists.
        
        Args:
            collection_id: TraceSet collection ID
            traceset_ids: List of traceset IDs
            pixel_indices: List of pixel indices (can be > 150)
            start_time: Start time (ISO 8601)
            end_time: End time (ISO 8601)
            batch_size: Pixels per batch (default: 50, max: 150)
            
        Returns:
            Combined list of TraceSetDataValuesDto from all batches
            
        Example:
            >>> # Get data for 500 pixels (auto-batched into 10 requests)
            >>> data = client.get_traceset_data_batched(
            ...     collection_id=1,
            ...     traceset_ids=[3],
            ...     pixel_indices=list(range(500)),
            ...     start_time="2025-05-01T00:00:00Z",
            ...     end_time="2025-05-01T23:59:59Z"
            ... )
        """
        if batch_size > MAX_RADAR_BATCH_SIZE:
            raise ValidationError(
                f"batch_size cannot exceed {MAX_RADAR_BATCH_SIZE}, got {batch_size}"
            )
        if batch_size <= 0:
            raise ValidationError(f"batch_size must be positive, got {batch_size}")
        
        all_results: List[Dict[str, Any]] = []
        total_batches = (len(pixel_indices) + batch_size - 1) // batch_size
        
        self._logger.debug(
            f"Batching {len(pixel_indices)} pixels into {total_batches} batches "
            f"of {batch_size}"
        )
        
        for i in range(0, len(pixel_indices), batch_size):
            batch = pixel_indices[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            self._logger.debug(
                f"Fetching batch {batch_num}/{total_batches} ({len(batch)} pixels)"
            )
            
            results = self.get_traceset_data(
                collection_id=collection_id,
                traceset_ids=traceset_ids,
                pixel_indices=batch,
                start_time=start_time,
                end_time=end_time,
            )
            all_results.extend(results)
        
        self._logger.info(
            f"Retrieved {len(all_results)} records across {total_batches} batches"
        )
        
        return all_results

    # ========================================================================
    # ALARMS
    # ========================================================================
    
    def get_alarms_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Get all alarms for a trace.
        
        GET /v1/alarms/overflow-detailed-info-by-trace?traceId=...
        
        Args:
            trace_id: Trace ID (int or convertible to int)
            
        Returns:
            List of AlarmDetailedInfoDto dictionaries
            
        Example:
            >>> alarms = client.get_alarms_for_trace(12345)
            >>> for alarm in alarms:
            ...     print(alarm['alarmType'], alarm['severity'])
        """
        trace_id_int = self._validate_id(trace_id, "trace_id")
        
        data = self._http.get(
            ep.ALARMS_OVERFLOW_BY_TRACE,
            params={"traceId": trace_id_int},
            allow_404=True,
            allow_403=True,
        )
        
        if data is None:
            return []
        
        return self._extract_items(data)

    def get_overflow_alarms_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Get only OverflowMonitoring alarms for a trace.
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of overflow alarms
        """
        alarms = self.get_alarms_for_trace(trace_id)
        return [a for a in alarms if a.get("alarmType") == "OverflowMonitoring"]

    def get_recency_alarms_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Get only DataRecency alarms for a trace.
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of recency alarms
        """
        alarms = self.get_alarms_for_trace(trace_id)
        return [a for a in alarms if a.get("alarmType") == "DataRecency"]

    def split_alarms_by_type(
        self,
        alarms: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Split alarms into categories by type.
        
        Args:
            alarms: List of AlarmDetailedInfoDto
            
        Returns:
            Dictionary with keys: "overflow", "recency", "other"
            
        Example:
            >>> all_alarms = client.get_alarms_for_trace(12345)
            >>> by_type = client.split_alarms_by_type(all_alarms)
            >>> print(f"{len(by_type['overflow'])} overflow alarms")
        """
        return {
            "overflow": [
                a for a in alarms if a.get("alarmType") == "OverflowMonitoring"
            ],
            "recency": [
                a for a in alarms if a.get("alarmType") == "DataRecency"
            ],
            "other": [
                a for a in alarms
                if a.get("alarmType") not in ("OverflowMonitoring", "DataRecency")
            ],
        }

    # ========================================================================
    # THRESHOLDS
    # ========================================================================
    
    def get_thresholds_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Get alarm thresholds configured for a trace.
        
        GET /v1/traces/{traceId}/thresholds
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of threshold configurations
        """
        trace_id_int = self._validate_id(trace_id, "trace_id")
        
        path = ep.TRACE_THRESHOLDS.format(trace_id=trace_id_int)
        data = self._http.get(path, allow_404=True, allow_403=True)
        
        if data is None:
            return []
        
        # API returns {"thresholds": [...]}
        if isinstance(data, dict) and "thresholds" in data:
            return data["thresholds"]
        
        return data if isinstance(data, list) else []

    # ========================================================================
    # PROJECT-LEVEL ALARMS
    # ========================================================================
    
    def get_detailed_alarms_by_project(
        self,
        project_id: int
    ) -> Dict[int, Dict[str, Any]]:
        """
        Get all alarms for a project, indexed by trace ID.
        
        GET /v1/alarms/detailed-by-project?projectId=...
        
        Args:
            project_id: Project ID
            
        Returns:
            Dictionary mapping trace_id -> alarm details
            
        Example:
            >>> alarms = client.get_detailed_alarms_by_project(594)
            >>> trace_12345_alarm = alarms.get(12345)
        """
        self._validate_id(project_id, "project_id")
        
        data = self._http.get(
            ep.ALARMS_DETAILED_BY_PROJECT,
            params={"projectId": int(project_id)},
            allow_404=True,
            allow_403=True,
        )
        
        if data is None:
            return {}
        
        alarms_list = self._extract_items(data)
        
        # Index by trace ID
        out: Dict[int, Dict[str, Any]] = {}
        for alarm in alarms_list:
            trace_id = alarm.get("traceId")
            if trace_id is not None:
                out[int(trace_id)] = alarm
        
        return out

    # ========================================================================
    # ARI (ANNUAL RECURRENCE INTERVAL)
    # ========================================================================
    
    def get_ari_data(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        ari_type: str = "Tp108",
    ) -> Any:
        """
        Get ARI (Annual Recurrence Interval) values for a trace.
        
        GET /v1/traces/{traceId}/ari
        
        Args:
            trace_id: Trace ID
            from_time: Start time (ISO 8601)
            to_time: End time (ISO 8601)
            ari_type: ARI calculation type (default: "Tp108")
            
        Returns:
            ARI data (structure depends on API response)
            
        Example:
            >>> ari_data = client.get_ari_data(
            ...     trace_id=12345,
            ...     from_time="2025-01-01T00:00:00Z",
            ...     to_time="2025-01-31T23:59:59Z"
            ... )
        """
        trace_id_int = self._validate_id(trace_id, "trace_id")
        self._validate_time_string(from_time, "from_time")
        self._validate_time_string(to_time, "to_time")
        
        path = ep.TRACE_ARI.format(trace_id=trace_id_int)
        params = {
            "from": from_time,
            "to": to_time,
            "type": ari_type
        }
        
        return self._http.get(path, params=params, allow_404=True)

    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _extract_items(self, data: Any) -> List[Dict[str, Any]]:
        """
        Extract items list from API response.
        
        Handles both:
        - {"items": [...]} responses
        - Direct list responses
        
        Args:
            data: API response (dict or list)
            
        Returns:
            List of items
        """
        if data is None:
            return []
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        if isinstance(data, list):
            return data
        return []

    def _validate_id(self, value: Any, param_name: str) -> int:
        """
        Validate and convert ID parameter to int.
        
        Args:
            value: Value to validate
            param_name: Parameter name for error message
            
        Returns:
            Integer value
            
        Raises:
            ValidationError: If value is invalid
        """
        try:
            id_int = int(value)
            if id_int <= 0:
                raise ValidationError(f"{param_name} must be positive, got {id_int}")
            return id_int
        except (ValueError, TypeError) as e:
            raise ValidationError(
                f"{param_name} must be convertible to int, got {type(value).__name__}: {value}"
            ) from e

    def _validate_time_string(self, time_str: str, param_name: str) -> None:
        """
        Validate ISO 8601 time string format.
        
        Args:
            time_str: Time string to validate
            param_name: Parameter name for error message
            
        Raises:
            ValidationError: If format is invalid
        """
        if not time_str or not isinstance(time_str, str):
            raise ValidationError(f"{param_name} must be a non-empty string")
        
        # Basic ISO 8601 format check
        if "T" not in time_str:
            raise ValidationError(
                f"{param_name} must be ISO 8601 format (e.g., '2025-01-01T00:00:00Z'), "
                f"got: {time_str}"
            )