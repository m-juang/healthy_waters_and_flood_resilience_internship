from __future__ import annotations
from typing import Any, Dict, List, Optional
from .http import MoataHttp
from . import endpoints as ep


class MoataClient:
    def __init__(self, http: MoataHttp) -> None:
        self._http = http

    def get_rain_gauges(self, project_id: int, asset_type_id: int) -> List[Dict[str, Any]]:
        path = ep.PROJECT_ASSETS.format(project_id=project_id)
        data = self._http.get(path, params={"assetTypeId": asset_type_id})
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_traces_for_asset(self, asset_id: Any) -> List[Dict[str, Any]]:
        data = self._http.get(ep.ASSET_TRACES, params={"assetId": asset_id})
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_overflow_alarms_for_trace(self, trace_id: Any) -> List[Dict[str, Any]]:
        data = self._http.get(ep.ALARMS_OVERFLOW_BY_TRACE, params={"traceId": trace_id}, allow_404=True, allow_403=True)
        if data is None:
            return []
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []

    def get_thresholds_for_trace(self, trace_id: Any) -> List[Dict[str, Any]]:
        path = ep.TRACE_THRESHOLDS.format(trace_id=trace_id)
        data = self._http.get(path, allow_404=True, allow_403=True)
        if data is None:
            return []
        if isinstance(data, dict) and "thresholds" in data:
            return data["thresholds"]
        return data if isinstance(data, list) else []

    def get_detailed_alarms_by_project(self, project_id: int) -> Dict[int, Dict[str, Any]]:
        data = self._http.get(ep.ALARMS_DETAILED_BY_PROJECT, params={"projectId": project_id}, allow_404=True, allow_403=True)
        if data is None:
            return {}

        alarms_list = data if isinstance(data, list) else data.get("items", [])
        out: Dict[int, Dict[str, Any]] = {}
        for a in alarms_list:
            tid = a.get("traceId")
            if tid is not None:
                out[int(tid)] = a
        return out
    
    def get_ari_data(
        self,
        trace_id: Any,
        from_time: str,
        to_time: str,
        ari_type: str = "Tp108"
    ) -> Any:
        """
        Get ARI (Annual Recurrence Interval) values for a trace.
        
        Args:
            trace_id: Trace ID
            from_time: Start time (ISO format UTC string)
            to_time: End time (ISO format UTC string)
            ari_type: ARI type ("Tp108", "HirdsV4", or "Hirds")
        
        Returns:
            ARI data
        """
        path = ep.TRACE_ARI.format(trace_id=trace_id)

        params = {
            "from": from_time,
            "to": to_time,
            "type": ari_type
        }
        return self._http.get(path, params=params, allow_404=True)