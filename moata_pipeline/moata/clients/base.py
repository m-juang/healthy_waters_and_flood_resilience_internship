"""Base client with common functionality for all specialized clients."""
from typing import Any, List, Dict, Union
import logging

from moata_pipeline.common.protocols import HTTPClientProtocol


class ValidationError(Exception):
    """Raised when parameter validation fails."""
    pass


class BaseClient:
    """
    Base client for Moata API with common functionality.
    
    All specialized clients inherit from this to get:
    - HTTP client access
    - Logging
    - Common validation methods
    - Response handling
    
    This promotes DRY (Don't Repeat Yourself) while keeping
    specialized clients focused on their domain.
    """
    
    def __init__(self, http: HTTPClientProtocol):
        """
        Initialize base client.
        
        Args:
            http: HTTP client implementing HTTPClientProtocol
            
        Raises:
            ValueError: If http is None
        """
        if http is None:
            raise ValueError("http cannot be None")
        
        self._http = http
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self._logger.debug(f"{self.__class__.__name__} initialized")
    
    def _log_request(self, method: str, endpoint: str) -> None:
        """Log outgoing request."""
        self._logger.debug(f"{method} {endpoint}")
    
    def _extract_items(self, data: Any) -> List[Dict[str, Any]]:
        """
        Extract items array from API response.
        
        Moata API often wraps data in {"data": {"items": [...]}}
        This helper extracts the items list regardless of wrapper.
        
        Args:
            data: Response from API
            
        Returns:
            List of items
        """
        if isinstance(data, list):
            return data
        
        if isinstance(data, dict):
            # Try data.items first
            if "data" in data and isinstance(data["data"], dict):
                return data["data"].get("items", [])
            # Try items directly
            if "items" in data:
                return data["items"]
        
        return []
    
    def _validate_id(self, value: Any, param_name: str) -> int:
        """
        Validate that value is a positive integer ID.
        
        Args:
            value: Value to validate
            param_name: Parameter name for error messages
            
        Returns:
            Validated integer ID
            
        Raises:
            ValidationError: If value is invalid
        """
        try:
            id_val = int(value)
            if id_val <= 0:
                raise ValidationError(f"{param_name} must be positive, got {id_val}")
            return id_val
        except (TypeError, ValueError) as e:
            raise ValidationError(f"{param_name} must be an integer, got {type(value).__name__}: {value}") from e
    
    def _validate_time_string(self, time_str: str, param_name: str) -> None:
        """
        Validate that time string is non-empty.
        
        Args:
            time_str: Time string to validate
            param_name: Parameter name for error messages
            
        Raises:
            ValidationError: If time string is invalid
        """
        if not isinstance(time_str, str):
            raise ValidationError(f"{param_name} must be a string, got {type(time_str).__name__}")
        
        if not time_str.strip():
            raise ValidationError(f"{param_name} cannot be empty")
