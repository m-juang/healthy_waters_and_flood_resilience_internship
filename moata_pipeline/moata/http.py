"""
Moata API HTTP Client Module

Provides HTTP client with rate limiting, automatic retries, and token refresh.
Handles network errors, timeouts, and API rate limits gracefully.

Usage:
    from moata_pipeline.moata.http import MoataHttp
    from moata_pipeline.moata.auth import MoataAuth
    
    auth = MoataAuth(...)
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url="https://api.moata.io",
        requests_per_second=2.0,
        verify_ssl=True
    )
    
    # Make request
    data = http.get("/projects/123/assets")

Features:
    - Client-side rate limiting (configurable RPS)
    - Automatic exponential backoff retries
    - Connection pooling for performance
    - Separate connect and read timeouts
    - Automatic token refresh on 401
    - Optional 404/403 handling

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import logging
import time
import warnings
from typing import Any, Callable, Dict, Optional, Tuple, Union

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Suppress SSL warnings only if verify_ssl=False
# (Instead of global disable, we'll handle per-instance)
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
DEFAULT_REQUESTS_PER_SECOND = 2.0
DEFAULT_VERIFY_SSL = True
DEFAULT_READ_TIMEOUT_SECONDS = 60
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 1.0
DEFAULT_POOL_CONNECTIONS = 20
DEFAULT_POOL_MAXSIZE = 20
DEFAULT_TOKEN_REFRESH_DELAY = 1  # seconds to wait after 401 before retry
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)


class HTTPError(Exception):
    """Base exception for HTTP errors."""
    pass


class RateLimitError(HTTPError):
    """Raised when rate limit is exceeded."""
    pass


class TimeoutError(HTTPError):
    """Raised when request times out."""
    pass


class AuthenticationError(HTTPError):
    """Raised when authentication fails."""
    pass


class MoataHttp:
    """
    HTTP client for Moata API with rate limiting and retry logic.
    
    Provides robust HTTP client with:
    - Automatic rate limiting
    - Exponential backoff retries
    - Connection pooling
    - Token refresh on 401
    - Configurable timeouts
    
    Attributes:
        _get_token: Function to get access token
        _base_url: Base URL for API
        _sleep: Sleep duration between requests (rate limiting)
        _verify_ssl: Whether to verify SSL certificates
        _timeout: Tuple of (connect_timeout, read_timeout)
        _session: Requests session with retry logic
        _request_count: Total number of requests made
        _retry_count: Total number of retries
        
    Example:
        >>> http = MoataHttp(
        ...     get_token_fn=auth.get_token,
        ...     base_url="https://api.moata.io",
        ...     requests_per_second=2.0
        ... )
        >>> data = http.get("/projects/123/assets")
    """
    
    def __init__(
        self,
        get_token_fn: Callable[[], str],
        base_url: str,
        requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND,
        verify_ssl: bool = DEFAULT_VERIFY_SSL,
        timeout_seconds: int = DEFAULT_READ_TIMEOUT_SECONDS,
        connect_timeout_seconds: int = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        pool_connections: int = DEFAULT_POOL_CONNECTIONS,
        pool_maxsize: int = DEFAULT_POOL_MAXSIZE,
    ) -> None:
        """
        Initialize Moata HTTP client.
        
        Args:
            get_token_fn: Function that returns valid access token
            base_url: Base URL for API (e.g., "https://api.moata.io")
            requests_per_second: Max requests per second (default: 2.0)
            verify_ssl: Whether to verify SSL certificates (default: True)
            timeout_seconds: Read timeout in seconds (default: 60)
            connect_timeout_seconds: Connect timeout in seconds (default: 15)
            max_retries: Max retry attempts for failed requests (default: 5)
            backoff_factor: Backoff factor for retries (default: 1.0)
            pool_connections: Connection pool size (default: 20)
            pool_maxsize: Max pool size (default: 20)
            
        Raises:
            ValueError: If parameters are invalid
        """
        # Validate parameters
        if not get_token_fn:
            raise ValueError("get_token_fn cannot be None")
        if not base_url:
            raise ValueError("base_url cannot be empty")
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        
        self._get_token = get_token_fn
        self._base_url = base_url.rstrip("/")
        self._sleep = 1.0 / max(requests_per_second, 0.1)
        self._verify_ssl = verify_ssl
        
        # Use tuple timeout: (connect, read)
        self._timeout: Tuple[int, int] = (connect_timeout_seconds, timeout_seconds)
        
        # Suppress SSL warnings if SSL verification is disabled
        if not verify_ssl:
            warnings.filterwarnings(
                'ignore',
                message='Unverified HTTPS request',
                category=urllib3.exceptions.InsecureRequestWarning
            )
        
        # Statistics tracking
        self._request_count = 0
        self._retry_count = 0
        
        # Setup logger
        self._logger = logging.getLogger(__name__)
        
        # Create session with retry logic
        self._session = self._create_session(
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize
        )
        
        # Log initialization
        self._logger.debug(
            f"MoataHttp initialized: base_url={base_url}, "
            f"rps={requests_per_second}, verify_ssl={verify_ssl}, "
            f"timeout={self._timeout}"
        )

    def _create_session(
        self,
        max_retries: int,
        backoff_factor: float,
        pool_connections: int,
        pool_maxsize: int
    ) -> requests.Session:
        """
        Create requests session with retry logic and connection pooling.
        
        Args:
            max_retries: Maximum retry attempts
            backoff_factor: Backoff factor for exponential backoff
            pool_connections: Number of connection pools
            pool_maxsize: Maximum size of each pool
            
        Returns:
            Configured requests.Session
        """
        session = requests.Session()
        
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=frozenset(["GET", "POST"]),  # Added POST
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
        )
        
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        allow_404: bool = False,
        allow_403: bool = False,
    ) -> Optional[Union[Dict, list]]:
        """
        Make GET request to API endpoint.
        
        Args:
            path: API path (e.g., "/projects/123/assets")
            params: Query parameters (optional)
            allow_404: Return None on 404 instead of raising (default: False)
            allow_403: Return None on 403 instead of raising (default: False)
            
        Returns:
            JSON response as dict/list, or None if allowed status code
            
        Raises:
            HTTPError: If request fails
            TimeoutError: If request times out
            AuthenticationError: If authentication fails
            ValueError: If response is not valid JSON
            
        Example:
            >>> data = http.get("/projects/123/assets")
            >>> data = http.get("/assets/456", allow_404=True)  # Returns None if not found
        """
        url = f"{self._base_url}/{path.lstrip('/')}"
        
        # Rate limiting (client-side)
        time.sleep(self._sleep)
        
        # Increment request counter
        self._request_count += 1
        
        # Prepare headers
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }
        
        # Log request
        self._logger.debug(f"GET {url} params={params}")
        
        try:
            resp = self._session.get(
                url,
                headers=headers,
                params=params,
                timeout=self._timeout,
                verify=self._verify_ssl,
            )
            
        except requests.exceptions.ConnectTimeout as e:
            raise TimeoutError(
                f"Connection timeout after {self._timeout[0]}s for {url}"
            ) from e
            
        except requests.exceptions.ReadTimeout as e:
            raise TimeoutError(
                f"Read timeout after {self._timeout[1]}s for {url}"
            ) from e
            
        except requests.exceptions.SSLError as e:
            raise HTTPError(
                f"SSL error for {url}: {e}"
            ) from e
            
        except requests.exceptions.ConnectionError as e:
            raise HTTPError(
                f"Connection error for {url}: {e}"
            ) from e
        
        # Handle optional status codes
        if resp.status_code == 404 and allow_404:
            self._logger.debug(f"404 Not Found (allowed): {url}")
            return None
            
        if resp.status_code == 403 and allow_403:
            self._logger.debug(f"403 Forbidden (allowed): {url}")
            return None
        
        # Handle 401 Unauthorized - refresh token and retry ONCE
        if resp.status_code == 401:
            self._logger.warning(
                f"401 Unauthorized for {url} - refreshing token and retrying"
            )
            self._retry_count += 1
            
            # Get fresh token
            headers["Authorization"] = f"Bearer {self._get_token()}"
            
            # Brief delay before retry
            time.sleep(DEFAULT_TOKEN_REFRESH_DELAY)
            
            # Retry request
            resp = self._session.get(
                url,
                headers=headers,
                params=params,
                timeout=self._timeout,
                verify=self._verify_ssl,
            )
            
            # Re-check optional status codes after retry
            if resp.status_code == 404 and allow_404:
                return None
            if resp.status_code == 403 and allow_403:
                return None
            
            # If still 401, authentication has failed
            if resp.status_code == 401:
                raise AuthenticationError(
                    f"Authentication failed for {url} even after token refresh"
                )
        
        # Check for rate limiting
        if resp.status_code == 429:
            retry_after = resp.headers.get('Retry-After', 'unknown')
            raise RateLimitError(
                f"Rate limit exceeded for {url}. Retry after: {retry_after}"
            )
        
        # Raise for other HTTP errors
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise HTTPError(
                f"HTTP {resp.status_code} for {url}: {resp.text[:200]}"
            ) from e
        
        # Handle empty response
        if not resp.content:
            self._logger.debug(f"Empty response for {url}")
            return None
        
        # Parse JSON
        try:
            return resp.json()
        except ValueError as e:
            self._logger.warning(
                f"Non-JSON response for {url} (status={resp.status_code}): "
                f"{resp.text[:500]}"
            )
            raise ValueError(
                f"Invalid JSON response from {url}: {resp.text[:200]}"
            ) from e

    def get_stats(self) -> Dict[str, int]:
        """
        Get HTTP client statistics.
        
        Returns:
            Dictionary with request and retry counts
            
        Example:
            >>> stats = http.get_stats()
            >>> print(stats)
            {'requests': 42, 'retries': 3}
        """
        return {
            "requests": self._request_count,
            "retries": self._retry_count,
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._request_count = 0
        self._retry_count = 0
        self._logger.debug("Statistics reset")