"""Client for alarm-related API operations."""
from typing import Dict, List, Any, Union

from .base import BaseClient
from .. import endpoints as ep


class AlarmClient(BaseClient):
    """
    Client for alarm operations (overflow, recency, thresholds).
    
    This client handles:
    - Fetching alarms for traces
    - Filtering alarms by type
    - Getting alarm thresholds
    - Project-level alarm queries
    
    Single Responsibility: Alarm management
    
    Example:
        >>> client = AlarmClient(http=http_client)
        >>> alarms = client.get_alarms_for_trace(trace_id=12345)
        >>> overflow = client.get_overflow_alarms_for_trace(trace_id=12345)
        >>> thresholds = client.get_thresholds_for_trace(trace_id=12345)
    """
    
    def get_alarms_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Get all alarms for a trace.
        
        Fetches detailed alarm information including type, severity,
        and configuration.
        
        Args:
            trace_id: Trace ID (int or convertible to int)
            
        Returns:
            List of AlarmDetailedInfoDto dictionaries
            
        Raises:
            ValidationError: If trace_id is invalid
            
        Example:
            >>> alarms = client.get_alarms_for_trace(12345)
            >>> for alarm in alarms:
            ...     print(f"{alarm['alarmType']}: {alarm['severity']}")
        """
        trace_id_int = self._validate_id(trace_id, "trace_id")
        
        self._log_request("GET", ep.ALARMS_OVERFLOW_BY_TRACE)
        
        data = self._http.get(
            ep.ALARMS_OVERFLOW_BY_TRACE,
            params={"traceId": trace_id_int},
            allow_404=True,
            allow_403=True,
        )
        
        if data is None:
            self._logger.debug(f"No alarms found for trace {trace_id_int}")
            return []
        
        alarms = self._extract_items(data)
        self._logger.info(f"Retrieved {len(alarms)} alarms for trace {trace_id_int}")
        return alarms
    
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
            
        Example:
            >>> overflow = client.get_overflow_alarms_for_trace(12345)
            >>> print(f"Found {len(overflow)} overflow alarms")
        """
        alarms = self.get_alarms_for_trace(trace_id)
        overflow = [a for a in alarms if a.get("alarmType") == "OverflowMonitoring"]
        
        self._logger.debug(
            f"Filtered {len(overflow)} overflow alarms from {len(alarms)} total"
        )
        return overflow
    
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
            
        Example:
            >>> recency = client.get_recency_alarms_for_trace(12345)
            >>> print(f"Found {len(recency)} data recency alarms")
        """
        alarms = self.get_alarms_for_trace(trace_id)
        recency = [a for a in alarms if a.get("alarmType") == "DataRecency"]
        
        self._logger.debug(
            f"Filtered {len(recency)} recency alarms from {len(alarms)} total"
        )
        return recency
    
    def split_alarms_by_type(
        self,
        alarms: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Split alarms into categories by type.
        
        Categorizes alarms into overflow, recency, and other types
        for easier processing.
        
        Args:
            alarms: List of AlarmDetailedInfoDto
            
        Returns:
            Dictionary with keys: "overflow", "recency", "other"
            
        Example:
            >>> all_alarms = client.get_alarms_for_trace(12345)
            >>> by_type = client.split_alarms_by_type(all_alarms)
            >>> print(f"{len(by_type['overflow'])} overflow")
            >>> print(f"{len(by_type['recency'])} recency")
            >>> print(f"{len(by_type['other'])} other")
        """
        categorized = {
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
        
        self._logger.debug(
            f"Split {len(alarms)} alarms: "
            f"{len(categorized['overflow'])} overflow, "
            f"{len(categorized['recency'])} recency, "
            f"{len(categorized['other'])} other"
        )
        
        return categorized
    
    def get_thresholds_for_trace(
        self,
        trace_id: Union[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Get alarm thresholds configured for a trace.
        
        Returns the threshold values that trigger alarms for this trace.
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of threshold configurations
            
        Raises:
            ValidationError: If trace_id is invalid
            
        Example:
            >>> thresholds = client.get_thresholds_for_trace(12345)
            >>> for t in thresholds:
            ...     print(f"{t['name']}: {t['value']} {t['unit']}")
        """
        trace_id_int = self._validate_id(trace_id, "trace_id")
        
        path = ep.TRACE_THRESHOLDS.format(trace_id=trace_id_int)
        self._log_request("GET", path)
        
        data = self._http.get(path, allow_404=True, allow_403=True)
        
        if data is None:
            self._logger.debug(f"No thresholds found for trace {trace_id_int}")
            return []
        
        # API returns {"thresholds": [...]}
        if isinstance(data, dict) and "thresholds" in data:
            thresholds = data["thresholds"]
        elif isinstance(data, list):
            thresholds = data
        else:
            thresholds = []
        
        self._logger.info(
            f"Retrieved {len(thresholds)} thresholds for trace {trace_id_int}"
        )
        return thresholds
    
    def get_detailed_alarms_by_project(
        self,
        project_id: Union[int, str]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Get all alarms for a project, indexed by trace ID.
        
        Useful for fetching alarms for all traces in a project
        at once, rather than making individual calls.
        
        Args:
            project_id: Project ID
            
        Returns:
            Dictionary mapping trace_id -> alarm details
            
        Raises:
            ValidationError: If project_id is invalid
            
        Example:
            >>> alarms = client.get_detailed_alarms_by_project(594)
            >>> print(f"Found alarms for {len(alarms)} traces")
            >>> trace_12345_alarm = alarms.get(12345)
            >>> if trace_12345_alarm:
            ...     print(f"Trace 12345 alarm: {trace_12345_alarm['severity']}")
        """
        project_id_int = self._validate_id(project_id, "project_id")
        
        self._log_request("GET", ep.ALARMS_DETAILED_BY_PROJECT)
        
        data = self._http.get(
            ep.ALARMS_DETAILED_BY_PROJECT,
            params={"projectId": project_id_int},
            allow_404=True,
            allow_403=True,
        )
        
        if data is None:
            self._logger.debug(f"No alarms found for project {project_id_int}")
            return {}
        
        alarms_list = self._extract_items(data)
        
        # Index by trace ID for easy lookup
        out: Dict[int, Dict[str, Any]] = {}
        for alarm in alarms_list:
            trace_id = alarm.get("traceId")
            if trace_id is not None:
                out[int(trace_id)] = alarm
        
        self._logger.info(
            f"Retrieved alarms for {len(out)} traces (project {project_id_int})"
        )
        return out
