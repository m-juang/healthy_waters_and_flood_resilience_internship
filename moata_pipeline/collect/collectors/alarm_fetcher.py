"""Alarm and threshold fetching logic."""
from typing import Any, Dict, List

from .base import BaseCollector, HTTPClientProtocol
from moata_pipeline.common.typing_utils import safe_int


class AlarmFetcher(BaseCollector):
    """
    Fetches alarm and threshold data.
    
    Single Responsibility: Alarm operations only
    
    Responsibilities:
    - Fetch alarms for traces
    - Fetch alarm thresholds
    - Fetch project-level detailed alarms
    - Split and categorize alarms
    
    Example:
        >>> fetcher = AlarmFetcher(client)
        >>> alarms = fetcher.fetch_alarms_for_trace(12345)
        >>> thresholds = fetcher.fetch_thresholds_for_trace(12345)
        >>> detailed = fetcher.fetch_detailed_alarms_by_project(594)
    """
    
    def fetch_alarms_for_trace(
        self,
        trace_id: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch alarms for a specific trace.
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of alarm dictionaries
            
        Raises:
            ValueError: If trace_id is invalid
            
        Example:
            >>> alarms = fetcher.fetch_alarms_for_trace(12345)
            >>> print(f"Found {len(alarms)} alarms")
        """
        self._validate_positive_int(trace_id, "trace_id")
        
        self._logger.debug(f"Fetching alarms for trace {trace_id}")
        
        try:
            alarms = self._client.get_alarms_for_trace(trace_id)
            
            self._logger.debug(f"  ✓ Got {len(alarms)} alarms")
            
            return alarms
            
        except Exception as e:
            self._logger.error(f"Failed to fetch alarms for trace {trace_id}: {e}")
            return []
    
    def fetch_thresholds_for_trace(
        self,
        trace_id: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch alarm thresholds for a trace.
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of threshold dictionaries
            
        Raises:
            ValueError: If trace_id is invalid
            
        Example:
            >>> thresholds = fetcher.fetch_thresholds_for_trace(12345)
            >>> for t in thresholds:
            ...     print(f"{t['name']}: {t['value']}")
        """
        self._validate_positive_int(trace_id, "trace_id")
        
        self._logger.debug(f"Fetching thresholds for trace {trace_id}")
        
        try:
            thresholds = self._client.get_thresholds_for_trace(trace_id)
            
            self._logger.debug(f"  ✓ Got {len(thresholds)} thresholds")
            
            return thresholds
            
        except Exception as e:
            self._logger.error(f"Failed to fetch thresholds for trace {trace_id}: {e}")
            return []
    
    def fetch_detailed_alarms_by_project(
        self,
        project_id: int
    ) -> Dict[int, Dict[str, Any]]:
        """
        Fetch all detailed alarms for a project.
        
        Returns alarms indexed by trace ID for efficient lookup.
        
        Args:
            project_id: Moata project ID
            
        Returns:
            Dictionary mapping trace_id -> alarm details
            
        Raises:
            ValueError: If project_id is invalid
            
        Example:
            >>> alarms_by_trace = fetcher.fetch_detailed_alarms_by_project(594)
            >>> alarm = alarms_by_trace.get(12345)
            >>> if alarm:
            ...     print(f"Trace 12345: {alarm['severity']}")
        """
        self._validate_positive_int(project_id, "project_id")
        
        self._logger.info("Fetching detailed alarms for project...")
        self._logger.info(f"  Project ID: {project_id}")
        
        try:
            detailed_by_trace = self._client.get_detailed_alarms_by_project(project_id)
            
            self._logger.info(f"✓ Fetched {len(detailed_by_trace)} detailed alarms")
            
            return detailed_by_trace
            
        except Exception as e:
            self._logger.error(f"Failed to fetch detailed alarms for project {project_id}: {e}")
            return {}
    
    def split_alarms_by_type(
        self,
        alarms: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Split alarms into categories by type.
        
        Args:
            alarms: List of alarm dictionaries
            
        Returns:
            Dictionary with keys: "overflow", "recency", "other"
            
        Example:
            >>> alarms = fetcher.fetch_alarms_for_trace(12345)
            >>> by_type = fetcher.split_alarms_by_type(alarms)
            >>> print(f"Overflow: {len(by_type['overflow'])}")
            >>> print(f"Recency: {len(by_type['recency'])}")
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
    
    def enrich_trace_with_alarms(
        self,
        trace: Dict[str, Any],
        detailed_by_trace: Dict[int, Dict[str, Any]],
        fetch_thresholds: bool = True
    ) -> Dict[str, Any]:
        """
        Enrich a trace with alarm and threshold data.
        
        Args:
            trace: Trace dictionary
            detailed_by_trace: Project-level alarm lookup
            fetch_thresholds: Whether to fetch thresholds
            
        Returns:
            Enriched trace dictionary with alarms and thresholds
            
        Example:
            >>> trace = {"id": 12345, "name": "Gauge A"}
            >>> enriched = fetcher.enrich_trace_with_alarms(trace, detailed_alarms)
            >>> print(enriched.keys())  # dict_keys(['id', 'name', 'alarms', 'thresholds'])
        """
        trace_id = safe_int(trace.get("id"))
        if trace_id is None:
            self._logger.warning(f"Trace has invalid ID: {trace.get('id')}")
            return trace
        
        # Add regular alarms
        alarms = self.fetch_alarms_for_trace(trace_id)
        trace["alarms"] = alarms
        
        # Add detailed alarm info if available
        if trace_id in detailed_by_trace:
            trace["detailedAlarm"] = detailed_by_trace[trace_id]
        
        # Add thresholds if requested
        if fetch_thresholds:
            thresholds = self.fetch_thresholds_for_trace(trace_id)
            trace["thresholds"] = thresholds
        
        return trace
