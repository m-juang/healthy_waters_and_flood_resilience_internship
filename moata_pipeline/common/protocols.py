"""
Protocol definitions for dependency injection and polymorphism.

Protocols define the interface contracts without forcing inheritance,
allowing for more flexible and testable code.

This module defines protocols for:
- HTTP clients
- Authentication providers
- Data collectors
- Data analyzers
- Report generators
- Cache managers

Example:
    >>> from moata_pipeline.common.protocols import DataCollectorProtocol
    >>> 
    >>> def process_data(collector: DataCollectorProtocol):
    ...     data = collector.collect()
    ...     # Process data...
"""
from typing import Protocol, Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path


class HTTPClientProtocol(Protocol):
    """
    Protocol for HTTP client implementations.
    
    Implementations must provide GET and POST methods for making
    HTTP requests. This allows for easy mocking in tests and
    swapping implementations (e.g., requests, httpx, aiohttp).
    """
    
    def get(self, endpoint: str, params: Optional[Dict] = None, **kwargs) -> Any:
        """
        Make GET request.
        
        Args:
            endpoint: API endpoint URL
            params: Query parameters
            **kwargs: Additional request options
            
        Returns:
            Response data (typically JSON)
        """
        ...
    
    def post(self, endpoint: str, data: Any = None, **kwargs) -> Any:
        """
        Make POST request.
        
        Args:
            endpoint: API endpoint URL
            data: Request body data
            **kwargs: Additional request options
            
        Returns:
            Response data (typically JSON)
        """
        ...


class AuthProviderProtocol(Protocol):
    """
    Protocol for authentication providers.
    
    Implementations must provide token management functionality.
    This allows for different authentication strategies (OAuth2,
    API keys, JWT, etc.) without changing client code.
    """
    
    def get_token(self) -> str:
        """
        Get valid access token.
        
        Returns:
            Valid access token string
        """
        ...
    
    def refresh_token(self) -> str:
        """
        Force token refresh and return new token.
        
        Returns:
            New access token string
        """
        ...


class DataCollectorProtocol(Protocol):
    """
    Protocol for data collectors.
    
    Implementations must provide a collect() method that retrieves
    data from a source. This allows for different collector types
    (gauge, radar, weather, etc.) to be used interchangeably.
    """
    
    def collect(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Collect data for specified time range.
        
        Args:
            start_time: Start of collection period (optional)
            end_time: End of collection period (optional)
            **kwargs: Additional collection options
            
        Returns:
            Dictionary containing collected data
        """
        ...


class DataAnalyzerProtocol(Protocol):
    """
    Protocol for data analyzers.
    
    Implementations must provide an analyze() method that processes
    data and returns analysis results. This allows for different
    analysis strategies to be plugged in.
    """
    
    def analyze(self, data: Any, **kwargs) -> Dict[str, Any]:
        """
        Analyze provided data.
        
        Args:
            data: Input data to analyze
            **kwargs: Analysis options
            
        Returns:
            Dictionary containing analysis results
        """
        ...


class ReportGeneratorProtocol(Protocol):
    """
    Protocol for report generators.
    
    Implementations must provide a generate() method that creates
    a report from data. This allows for different output formats
    (HTML, PDF, CSV, etc.) without changing the analysis code.
    """
    
    def generate(self, data: Any, output_path: Path) -> Path:
        """
        Generate report from data.
        
        Args:
            data: Input data for report
            output_path: Where to save the report
            
        Returns:
            Path to generated report file
        """
        ...


class CacheManagerProtocol(Protocol):
    """
    Protocol for cache managers.
    
    Implementations must provide get/set/invalidate methods for
    caching operations. This allows for different caching strategies
    (memory, disk, Redis, etc.) to be swapped easily.
    """
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get cached value.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value if exists, None otherwise
        """
        ...
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set cached value with optional TTL.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (optional)
        """
        ...
    
    def invalidate(self, key: str) -> None:
        """
        Invalidate cache entry.
        
        Args:
            key: Cache key to invalidate
        """
        ...


class ValidationProtocol(Protocol):
    """
    Protocol for validators.
    
    Implementations must provide a validate() method that checks
    if input meets requirements and raises appropriate errors.
    """
    
    def validate(self, data: Any) -> bool:
        """
        Validate input data.
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid
            
        Raises:
            ValidationError: If data is invalid
        """
        ...


class OutputWriterProtocol(Protocol):
    """
    Protocol for output writers.
    
    Implementations must provide write methods for different formats.
    This allows for consistent output handling across the pipeline.
    """
    
    def write(self, data: Any, path: Path) -> Path:
        """
        Write data to file.
        
        Args:
            data: Data to write
            path: Output file path
            
        Returns:
            Path to written file
        """
        ...
