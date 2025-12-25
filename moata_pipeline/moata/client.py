from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from .http import MoataHttp
from . import endpoints as ep


class MoataClient:
    def __init__(self, http: MoataHttp) -> None:
        self._http = http

    # -----------------------------
    # Assets
    # -----------------------------
    def get_rain_gauges(self, project_id: int, asset_type_id: int) -> List[Dict[str, Any]]:
        """
        GET /v1/projects/{projectId}/assets?assetTypeId=...
        Swagger: returns a list of AssetWithGeometryDto.
        """
        path = ep.PROJECT_ASSETS.format(project_id=int(project_id))
        data = self._http.get(path, params={"assetTypeId": int(asset_type_id)})

        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_assets_with_geometry(
        self,
        project_id: int,
        asset_type_id: Optional[int] = None,
        sr_id: int = 4326,
        asset_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        GET /v1/projects/{projectId}/assets
        
        Get assets with geometry for a project.
        
        Args:
            project_id: Project ID
            asset_type_id: Optional asset type filter (e.g., 3541 for stormwater catchments)
            sr_id: Spatial Reference ID for geometry (default 4326 = WGS84)
            asset_name: Optional filter by asset name
            
        Returns:
            List of AssetWithGeometryDto with geometryWkt field
        """
        path = ep.PROJECT_ASSETS.format(project_id=int(project_id))
        params: Dict[str, Any] = {"srId": sr_id}
        
        if asset_type_id is not None:
            params["assetTypeId"] = int(asset_type_id)
        if asset_name is not None:
            params["assetName"] = asset_name
            
        data = self._http.get(path, params=params)

        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    # -----------------------------
    # Traces
    # -----------------------------
    def get_traces_for_asset(self, asset_id: Any) -> List[Dict[str, Any]]:
        """
        GET /v1/assets/traces?assetId=<array>
        """
        asset_id_int = int(asset_id)
        params = {"assetId": [asset_id_int]}
        data = self._http.get(ep.ASSET_TRACES, params=params)

        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_traces_for_assets(
        self,
        asset_ids: List[int],
        data_variable_type_id: Optional[int] = None,
        scenario_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Batch version: GET /v1/assets/traces?assetId=1&assetId=2...
        """
        params: Dict[str, Any] = {"assetId": [int(x) for x in asset_ids]}

        if data_variable_type_id is not None:
            params["dataVariableTypeId"] = int(data_variable_type_id)
        if scenario_id is not None:
            params["scenarioId"] = int(scenario_id)

        data = self._http.get(ep.ASSET_TRACES, params=params)

        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    # -----------------------------
    # Trace Data (Time Series)
    # -----------------------------
    def get_trace_data(
        self,
        trace_id: Any,
        from_time: str,
        to_time: str,
        data_type: str = "None",
        data_interval: Optional[int] = None,
        pad_with_zeroes: bool = False,
    ) -> Dict[str, Any]:
        """
        GET /v1/traces/{traceId}/data/utc
        
        Fetch time series data for a trace.
        """
        path = ep.TRACE_DATA_UTC.format(trace_id=int(trace_id))

        params: Dict[str, Any] = {
            "from": from_time,
            "to": to_time,
            "dataType": data_type,
            "padWithZeroes": str(pad_with_zeroes).lower(),
        }

        if data_interval is not None:
            params["dataInterval"] = int(data_interval)

        data = self._http.get(path, params=params, allow_404=True)

        if data is None:
            return {"items": [], "pageNumber": 0, "itemsPerPage": 0, "totalItems": 0}

        return data if isinstance(data, dict) else {"items": data if isinstance(data, list) else []}

    def get_trace_data_as_list(
        self,
        trace_id: Any,
        from_time: str,
        to_time: str,
        data_type: str = "None",
        data_interval: Optional[int] = None,
        pad_with_zeroes: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Convenience wrapper that returns just the items list.
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

    # -----------------------------
    # Radar / TraceSet Collections
    # -----------------------------
    def get_pixel_mappings_for_geometry(
        self,
        collection_id: int,
        wkt: str,
        sr_id: int = 4326,
    ) -> List[Dict[str, Any]]:
        """
        GET /v1/trace-set-collections/{traceSetCollectionId}/pixel-mappings/intersects-geometry
        
        Get pixel indices that intersect with a geometry.
        
        Args:
            collection_id: TraceSet collection ID (e.g., 1 for radar QPE)
            wkt: Well-Known Text geometry string
            sr_id: Spatial Reference ID (default 4326 = WGS84)
            
        Returns:
            List of TraceSetPixelMappingDto:
            [{"pixelIndex": int, "geometryWkt": str}, ...]
        """
        path = ep.TRACESET_PIXEL_MAPPINGS.format(collection_id=int(collection_id))
        params = {
            "wkt": wkt,
            "srId": sr_id,
        }
        
        data = self._http.get(path, params=params, allow_404=True)
        
        if data is None:
            return []
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_traceset_data(
        self,
        collection_id: int,
        traceset_ids: List[int],
        pixel_indices: List[int],
        start_time: str,
        end_time: str,
    ) -> List[Dict[str, Any]]:
        """
        GET /v1/trace-set-collections/{traceSetCollectionId}/trace-sets/data
        
        Get radar data values for specified tracesets and pixels.
        
        Args:
            collection_id: TraceSet collection ID (e.g., 1 for radar QPE)
            traceset_ids: List of traceset IDs (e.g., [3] for QPE)
            pixel_indices: List of pixel indices (max 150 per request recommended)
            start_time: Start time (ISO format, e.g., "2025-05-01T00:00:00Z")
            end_time: End time (ISO format, e.g., "2025-05-01T23:59:59Z")
            
        Returns:
            List of TraceSetDataValuesDto:
            [{
                "traceSetId": int,
                "traceSetPixelMappingId": int,
                "pixelIndex": int,
                "startTime": str,
                "endTime": str,
                "dataOffsetSeconds": int,
                "values": [float, ...]
            }, ...]
            
        Notes:
            - Limit: max 150 pixels per request
            - Recommended: batch in groups of 50 pixels
            - Recommended: max 24 hours of data per request
            - Data is minute resolution per pixel
        """
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
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_traceset_data_batched(
        self,
        collection_id: int,
        traceset_ids: List[int],
        pixel_indices: List[int],
        start_time: str,
        end_time: str,
        batch_size: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Batched version of get_traceset_data for large pixel lists.
        
        Args:
            collection_id: TraceSet collection ID
            traceset_ids: List of traceset IDs
            pixel_indices: List of pixel indices (can be > 150)
            start_time: Start time (ISO format)
            end_time: End time (ISO format)
            batch_size: Pixels per batch (default 50, max 150)
            
        Returns:
            Combined list of TraceSetDataValuesDto from all batches
        """
        all_results: List[Dict[str, Any]] = []
        
        # Batch pixels
        for i in range(0, len(pixel_indices), batch_size):
            batch = pixel_indices[i:i + batch_size]
            results = self.get_traceset_data(
                collection_id=collection_id,
                traceset_ids=traceset_ids,
                pixel_indices=batch,
                start_time=start_time,
                end_time=end_time,
            )
            all_results.extend(results)
            
        return all_results

    # -----------------------------
    # Alarms
    # -----------------------------
    def get_alarms_for_trace(self, trace_id: Any) -> List[Dict[str, Any]]:
        """
        GET /v1/alarms/overflow-detailed-info-by-trace?traceId=...
        """
        trace_id_int = int(trace_id)
        data = self._http.get(
            ep.ALARMS_OVERFLOW_BY_TRACE,
            params={"traceId": trace_id_int},
            allow_404=True,
            allow_403=True,
        )

        if data is None:
            return []

        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_overflow_alarms_for_trace(self, trace_id: Any) -> List[Dict[str, Any]]:
        """
        Returns only OverflowMonitoring alarms for the trace.
        """
        alarms = self.get_alarms_for_trace(trace_id)
        return [a for a in alarms if a.get("alarmType") == "OverflowMonitoring"]

    def get_recency_alarms_for_trace(self, trace_id: Any) -> List[Dict[str, Any]]:
        """
        Returns only DataRecency alarms for the trace.
        """
        alarms = self.get_alarms_for_trace(trace_id)
        return [a for a in alarms if a.get("alarmType") == "DataRecency"]

    def split_alarms_by_type(self, alarms: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Split any list of AlarmDetailedInfoDto into types.
        """
        return {
            "overflow": [a for a in alarms if a.get("alarmType") == "OverflowMonitoring"],
            "recency": [a for a in alarms if a.get("alarmType") == "DataRecency"],
            "other": [
                a for a in alarms
                if a.get("alarmType") not in ("OverflowMonitoring", "DataRecency")
            ],
        }

    # -----------------------------
    # Thresholds
    # -----------------------------
    def get_thresholds_for_trace(self, trace_id: Any) -> List[Dict[str, Any]]:
        path = ep.TRACE_THRESHOLDS.format(trace_id=int(trace_id))
        data = self._http.get(path, allow_404=True, allow_403=True)

        if data is None:
            return []

        if isinstance(data, dict) and "thresholds" in data:
            return data["thresholds"]
        return data if isinstance(data, list) else []

    # -----------------------------
    # Project-level alarms
    # -----------------------------
    def get_detailed_alarms_by_project(self, project_id: int) -> Dict[int, Dict[str, Any]]:
        data = self._http.get(
            ep.ALARMS_DETAILED_BY_PROJECT,
            params={"projectId": int(project_id)},
            allow_404=True,
            allow_403=True,
        )
        if data is None:
            return {}

        alarms_list = data if isinstance(data, list) else data.get("items", [])
        out: Dict[int, Dict[str, Any]] = {}
        for a in alarms_list:
            tid = a.get("traceId")
            if tid is not None:
                out[int(tid)] = a
        return out

    # -----------------------------
    # ARI
    # -----------------------------
    def get_ari_data(
        self,
        trace_id: Any,
        from_time: str,
        to_time: str,
        ari_type: str = "Tp108",
    ) -> Any:
        """
        Get ARI (Annual Recurrence Interval) values for a trace.
        """
        path = ep.TRACE_ARI.format(trace_id=int(trace_id))
        params = {"from": from_time, "to": to_time, "type": ari_type}
        return self._http.get(path, params=params, allow_404=True)