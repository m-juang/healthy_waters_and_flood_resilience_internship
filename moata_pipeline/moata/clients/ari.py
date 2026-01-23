"""Client for ARI (Average Recurrence Interval) API operations."""
from typing import Any, Union

from .base import BaseClient
from .. import endpoints as ep


class ARIClient(BaseClient):
    """
    Client for ARI (Average Recurrence Interval) operations.
    
    This client handles:
    - Fetching ARI data for traces
    - ARI calculations (Tp108, etc.)
    
    Single Responsibility: ARI data management
    
    ARI represents the statistical return period of rainfall events,
    used to assess flood risk and alarm severity.
    
    Example:
        >>> client = ARIClient(http=http_client)
        >>> ari_data = client.get_ari_data(
        ...     trace_id=12345,
        ...     from_time="2025-01-01T00:00:00Z",
        ...     to_time="2025-01-31T23:59:59Z",
        ...     ari_type="Tp108"
        ... )
    """
    
    def get_ari_data(
        self,
        trace_id: Union[int, str],
        from_time: str,
        to_time: str,
        ari_type: str = "Tp108",
    ) -> Any:
        """
        Get ARI (Average Recurrence Interval) values for a trace.
        
        ARI data shows the return period (in years) for rainfall events,
        helping assess if current conditions exceed expected thresholds.
        
        Args:
            trace_id: Trace ID
            from_time: Start time (ISO 8601 format)
            to_time: End time (ISO 8601 format)
            ari_type: ARI calculation type (default: "Tp108")
            
        Returns:
            ARI data (structure depends on API response)
            
        Raises:
            ValidationError: If parameters are invalid
            
        Example:
            >>> ari_data = client.get_ari_data(
            ...     trace_id=12345,
            ...     from_time="2025-01-01T00:00:00Z",
            ...     to_time="2025-01-31T23:59:59Z",
            ...     ari_type="Tp108"
            ... )
            >>> # Process ARI values to find exceedances
            >>> print(f"Max ARI: {max(ari_data.get('values', []))}")
        """
        # Validate parameters
        trace_id_int = self._validate_id(trace_id, "trace_id")
        self._validate_time_string(from_time, "from_time")
        self._validate_time_string(to_time, "to_time")
        
        # Build URL and params
        url = ep.TRACE_ARI.format(trace_id=trace_id_int)
        params = {
            "from": from_time,
            "to": to_time,
            "type": ari_type
        }
        self._log_request("GET", url)
        
        # Make request
        data = self._http.get(url, params=params, allow_404=True)
        
        if data is None:
            self._logger.warning(
                f"No ARI data returned for trace {trace_id_int} "
                f"(type: {ari_type})"
            )
            return None
        
        # Log summary if possible
        if isinstance(data, dict):
            values = data.get("values", [])
            if values:
                self._logger.info(
                    f"Retrieved ARI data for trace {trace_id_int}: "
                    f"{len(values)} values (type: {ari_type})"
                )
            else:
                self._logger.info(
                    f"Retrieved ARI data for trace {trace_id_int} "
                    f"(type: {ari_type}, no values)"
                )
        else:
            self._logger.info(
                f"Retrieved ARI data for trace {trace_id_int} (type: {ari_type})"
            )
        
        return data
