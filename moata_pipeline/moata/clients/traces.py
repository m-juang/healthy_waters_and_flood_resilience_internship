"""Client for trace-related API operations."""
from typing import Dict, List, Any, Union, Optional

from .base import BaseClient
from .. import endpoints as ep


class TraceClient(BaseClient):
    """
    Client for trace operations (timeseries metadata and data).
    
    This client handles:
    - Fetching trace metadata for assets
    - Getting trace data (values)
    - Converting trace data to lists
    
    Single Responsibility: Trace/timeseries management
    
    Example:
        >>> client = TraceClient(http=http_client)
        >>> traces = client.get_traces_for_asset(asset_id=12345)
        >>> data = client.get_trace_data(
        ...     trace_id=67890,
        ...     from_time="2025-01-01T00:00:00Z",
        ...     to_time="2025-01-31T23:59:59Z"
        ... )
    """
    
    def get_traces_for_asset(self, asset_id: Union[int, str]) -> List[Dict[str, Any]]:
        """
        Get all traces (timeseries) for an asset.
        
        Args:
            asset_id: Moata asset ID
            
        Returns:
            List of trace dictionaries with metadata
            
        Raises:
            ValidationError: If asset_id is invalid
            
        Example:
            >>> traces = client.get_traces_for_asset(12345)
            >>> for trace in traces:
            ...     print(f"{trace['name']}: {trace['id']}")
        """
        aid = self._validate_id(asset_id, "asset_id")
        
        params = {"assetId": [aid]}
        self._log_request("GET", ep.ASSET_TRACES)
        
        response = self._http.get(ep.ASSET_TRACES, params=params)
        traces = self._extract_items(response)
        
        self._logger.info(f"Retrieved {len(traces)} traces for asset {aid}")
        return traces
    
    def get_traces_for_assets(
        self,
        asset_ids: List[Union[int, str]],
        max_workers: int = 10
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Get traces for multiple assets concurrently.
        
        Args:
            asset_ids: List of asset IDs
            max_workers: Maximum concurrent requests
            
        Returns:
            Dictionary mapping asset_id -> list of traces
            
        Example:
            >>> traces_map = client.get_traces_for_assets([123, 456, 789])
            >>> for asset_id, traces in traces_map.items():
            ...     print(f"Asset {asset_id}: {len(traces)} traces")
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        self._logger.info(
            f"Fetching traces for {len(asset_ids)} assets "
            f"(max_workers={max_workers})"
        )
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all requests
            future_to_asset = {
                executor.submit(self.get_traces_for_asset, aid): aid
                for aid in asset_ids
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_asset):
                asset_id = future_to_asset[future]
                try:
                    traces = future.result()
                    results[int(asset_id)] = traces
                except Exception as e:
                    self._logger.error(
                        f"Failed to get traces for asset {asset_id}: {e}"
                    )
                    results[int(asset_id)] = []
        
        self._logger.info(f"Retrieved traces for {len(results)} assets")
        return results
    
    def get_trace_data(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        data_type: str = "None",
        data_interval: int = 300,
        pad_with_zeroes: bool = False
    ) -> Dict[str, Any]:
        """
        Get trace data (timeseries values) for a time range.
        
        Uses the /v1/traces/{traceId}/data/utc endpoint.
        
        **Request Limits (per Sam's email):**
        - Virtual traces: Limited to 32 days of data
        - Non-virtual traces: Limited to 46,080 data points
          (e.g., 32 days @ 1-minute resolution, 160 days @ 5-minute resolution)
        
        Args:
            trace_id: Moata trace ID
            from_time: Start time (ISO 8601 format, e.g., "2025-01-01T00:00:00Z")
            to_time: End time (ISO 8601 format, e.g., "2025-01-31T23:59:59Z")
            data_type: Summary algorithm. Valid options:
                - "None": Raw data (default)
                - "Mean", "Maximum", "Minimum": Statistical summaries
                - "Start", "End", "First", "Last": Position-based
                - "Total": Sum of values (required for rain gauges)
            data_interval: Period between data points in seconds (default: 300 = 5 min)
                - Use small values (e.g., 300) for raw data
                - Must be specified for API to return data
            pad_with_zeroes: If True, missing values are replaced with zeros
            
        Returns:
            Dictionary with trace data including:
            - items: List of data points with whenRecordedUnixSeconds, value, qualityCodeId
            - pageNumber, itemsPerPage, totalItems: Pagination info
            
        Raises:
            ValidationError: If parameters are invalid
            APIError: If API returns 400 (e.g., invalid date range) or 404 (trace not found)
            
        Example:
            >>> # Get raw 5-minute data for last 24 hours
            >>> data = client.get_trace_data(
            ...     trace_id=12345,
            ...     from_time="2025-01-20T00:00:00Z",
            ...     to_time="2025-01-21T00:00:00Z",
            ...     data_type="None",
            ...     data_interval=300  # 5 minutes
            ... )
            >>> for item in data.get('items', []):
            ...     print(f"{item['whenRecordedUnixSeconds']}: {item['value']}")
        """
        # Validate parameters
        tid = self._validate_id(trace_id, "trace_id")
        self._validate_time_string(from_time, "from_time")
        self._validate_time_string(to_time, "to_time")
        
        # Build URL
        path = ep.TRACE_DATA_UTC.format(trace_id=tid)
        params = {
            "from": from_time,
            "to": to_time,
            "dataType": data_type,
            "dataInterval": data_interval,
        }
        
        # Add optional parameter
        if pad_with_zeroes:
            params["padWithZeroes"] = "true"
        
        self._log_request("GET", path)
        
        # Make request
        response = self._http.get(path, params=params)
        
        # Extract data
        if isinstance(response, dict) and "data" in response:
            data = response["data"]
        else:
            data = response
        
        # Log summary - use 'items' key per API spec
        items = data.get("items", []) if isinstance(data, dict) else []
        self._logger.debug(
            f"Retrieved {len(items)} data points for trace {tid}"
        )
        
        return data
    
    def get_trace_data_as_list(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        pad_with_zeroes: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get trace data as list of {time, value} dictionaries.
        
        Convenience method that returns data in a simpler format.
        
        Args:
            trace_id: Moata trace ID
            from_time: Start time (ISO 8601 format)
            to_time: End time (ISO 8601 format)
            pad_with_zeroes: If True, use 0 for null values
            
        Returns:
            List of dictionaries with 'time' and 'value' keys
            
        Example:
            >>> data = client.get_trace_data_as_list(
            ...     trace_id=12345,
            ...     from_time="2025-01-01T00:00:00Z",
            ...     to_time="2025-01-31T23:59:59Z"
            ... )
            >>> for point in data[:5]:
            ...     print(f"{point['time']}: {point['value']}")
        """
        # Get raw data with default 5-minute interval
        data = self.get_trace_data(
            trace_id=trace_id,
            from_time=from_time,
            to_time=to_time,
            data_type="None",
            data_interval=300,
            pad_with_zeroes=pad_with_zeroes
        )
        
        if not isinstance(data, dict):
            return []
        
        # API returns 'items' array with {whenRecordedUnixSeconds, value, qualityCodeId}
        items = data.get("items", [])
        
        if not items:
            return []
        
        # Convert to simpler format
        result = []
        for item in items:
            time_unix = item.get("whenRecordedUnixSeconds")
            value = item.get("value")
            
            if value is None and pad_with_zeroes:
                value = 0
            
            result.append({
                "time": time_unix,
                "value": value,
                "quality": item.get("qualityCodeId")
            })
        
        return result

    def get_project_traces_info(
        self,
        project_id: Union[int, str],
        data_variable_type_id: Optional[int] = None,
        description: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get trace information for a project with optional filtering.
        
        This endpoint allows efficient filtering by dataVariableTypeId and description,
        useful for identifying specific trace types (e.g., "Rainfall" traces) without
        fetching all traces for each asset individually.
        
        Implements Sam's optimization suggestion for pre-filtering inactive gauges.
        
        Args:
            project_id: Moata project ID
            data_variable_type_id: Filter by data variable type (e.g., 10 for rainfall)
            description: Filter by trace description (e.g., "Rainfall")
            
        Returns:
            List of trace info dictionaries with assetId, telemeteredMaximumTime, etc.
            
        Example:
            >>> traces = client.get_project_traces_info(
            ...     project_id=594,
            ...     data_variable_type_id=10,
            ...     description="Rainfall"
            ... )
            >>> for trace in traces:
            ...     print(f"Asset {trace['assetId']}: {trace.get('telemeteredMaximumTime')}")
        """
        pid = self._validate_id(project_id, "project_id")
        
        url = ep.PROJECT_TRACES_INFO.format(project_id=pid)
        params: Dict[str, Any] = {}
        
        if data_variable_type_id is not None:
            params["dataVariableTypeId"] = data_variable_type_id
        if description is not None:
            params["description"] = description
        
        self._log_request("GET", url)
        
        response = self._http.get(url, params=params)
        traces = self._extract_items(response)
        
        self._logger.info(
            f"Retrieved {len(traces)} traces info for project {pid} "
            f"(filter: dvtId={data_variable_type_id}, desc='{description}')"
        )
        return traces
