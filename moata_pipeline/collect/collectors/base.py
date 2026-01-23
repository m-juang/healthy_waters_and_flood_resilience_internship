"""Base collector class with common functionality."""
import logging
from typing import Protocol, Any, Dict, List, Optional
from datetime import datetime


class HTTPClientProtocol(Protocol):
    """Protocol for HTTP client (MoataClient interface)."""
    
    def get_rain_gauges(self, project_id: int, asset_type_id: int) -> List[Dict[str, Any]]: ...
    def get_traces_for_asset(self, asset_id: int) -> List[Dict[str, Any]]: ...
    def get_trace_data(
        self,
        trace_id: int,
        from_time: str,
        to_time: str,
        data_type: str = "None",
        data_interval: int = 300,
        pad_with_zeroes: bool = False
    ) -> Dict[str, Any]: ...
    def get_alarms_for_trace(self, trace_id: int) -> List[Dict[str, Any]]: ...
    def get_thresholds_for_trace(self, trace_id: int) -> List[Dict[str, Any]]: ...
    def get_detailed_alarms_by_project(self, project_id: int) -> Dict[int, Dict[str, Any]]: ...


class BaseCollector:
    """
    Base class for all specialized collectors.
    
    Provides common functionality:
    - Logging setup
    - Client access
    - Validation utilities
    
    Single Responsibility: Common collector infrastructure
    """
    
    def __init__(self, client: HTTPClientProtocol, logger_name: Optional[str] = None) -> None:
        """
        Initialize base collector.
        
        Args:
            client: HTTP client implementing required protocol
            logger_name: Optional logger name (defaults to class name)
        """
        self._client = client
        
        if logger_name is None:
            logger_name = f"{__name__}.{self.__class__.__name__}"
        
        self._logger = logging.getLogger(logger_name)
    
    def _validate_positive_int(self, value: Any, name: str) -> int:
        """
        Validate that value is a positive integer.
        
        Args:
            value: Value to validate
            name: Parameter name for error message
            
        Returns:
            Validated integer
            
        Raises:
            ValueError: If value is not a positive integer
        """
        if not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be positive int, got {value}")
        return value
    
    def _validate_time_range(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """
        Validate time range.
        
        Args:
            start_time: Start time
            end_time: End time
            
        Raises:
            ValueError: If time range is invalid
        """
        if start_time >= end_time:
            raise ValueError(
                f"start_time must be before end_time: "
                f"{start_time} >= {end_time}"
            )
    
    def _log_time_range(self, start_time: datetime, end_time: datetime) -> None:
        """Log time range information."""
        self._logger.info("  Time Range:")
        self._logger.info(f"    Start: {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self._logger.info(f"    End:   {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self._logger.info(
            f"    Duration: {(end_time - start_time).total_seconds() / 3600:.1f} hours"
        )
