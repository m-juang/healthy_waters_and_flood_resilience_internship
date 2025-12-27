"""
Moata API OAuth2 Authentication Module

Handles OAuth2 client credentials flow authentication for the Moata API.
Implements token caching, automatic refresh, and retry logic for robust authentication.

Usage:
    from moata_pipeline.moata.auth import MoataAuth
    
    auth = MoataAuth(
        token_url="https://api.moata.io/oauth2/token",
        scope="https://moata.onmicrosoft.com/moata.io/.default",
        client_id="your_client_id",
        client_secret="your_client_secret",
        verify_ssl=True
    )
    
    # Get token (automatically refreshes if expired)
    token = auth.get_token()

Features:
    - Automatic token caching and refresh
    - Configurable expiry buffer (default: 5 minutes before expiry)
    - Exponential backoff retry logic for network failures
    - SSL verification support (configurable)
    - Connection error recovery

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_TTL_SECONDS = 3600  # 1 hour
DEFAULT_REFRESH_BUFFER_SECONDS = 300  # 5 minutes
DEFAULT_RETRY_ATTEMPTS = 5
DEFAULT_BACKOFF_FACTOR = 1.0  # 1s, 2s, 4s, 8s, 16s
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
CONNECTION_ERROR_RETRY_DELAY = 2  # seconds


class AuthenticationError(Exception):
    """Raised when authentication fails."""
    pass


class TokenRefreshError(Exception):
    """Raised when token refresh fails."""
    pass


@dataclass
class Token:
    """
    OAuth2 access token with expiry tracking.
    
    Attributes:
        access_token: The JWT access token string
        acquired_at: Unix timestamp when token was acquired
        ttl_seconds: Time-to-live in seconds (default: 3600)
    """
    access_token: str
    acquired_at: float
    ttl_seconds: int = DEFAULT_TTL_SECONDS

    def expires_in(self) -> float:
        """
        Calculate seconds until token expires.
        
        Returns:
            Seconds remaining until expiry (negative if already expired)
        """
        return self.ttl_seconds - (time.time() - self.acquired_at)

    def near_expiry(self, buffer_seconds: int = DEFAULT_REFRESH_BUFFER_SECONDS) -> bool:
        """
        Check if token is near expiry.
        
        Args:
            buffer_seconds: Refresh buffer in seconds (default: 300)
            
        Returns:
            True if token expires within buffer_seconds
            
        Example:
            >>> token.near_expiry(300)  # True if expires in <5 minutes
        """
        return self.expires_in() <= buffer_seconds

    def is_expired(self) -> bool:
        """
        Check if token has already expired.
        
        Returns:
            True if token is expired
        """
        return self.expires_in() <= 0


class MoataAuth:
    """
    OAuth2 client credentials authentication for Moata API.
    
    Manages access token acquisition, caching, and automatic refresh.
    Implements retry logic for robust authentication in unreliable networks.
    
    Attributes:
        _token_url: OAuth2 token endpoint URL
        _scope: OAuth2 scope for API access
        _client_id: OAuth2 client ID
        _client_secret: OAuth2 client secret
        _verify_ssl: Whether to verify SSL certificates
        _timeout: Request timeout in seconds
        _ttl: Token time-to-live in seconds
        _buffer: Refresh buffer in seconds
        _token: Cached access token (if available)
        
    Example:
        >>> auth = MoataAuth(
        ...     token_url="https://api.moata.io/oauth2/token",
        ...     scope="https://moata.onmicrosoft.com/moata.io/.default",
        ...     client_id="client_id",
        ...     client_secret="secret",
        ...     verify_ssl=True
        ... )
        >>> token = auth.get_token()
        >>> print(f"Token: {token[:20]}...")
    """
    
    def __init__(
        self,
        token_url: str,
        scope: str,
        client_id: str,
        client_secret: str,
        verify_ssl: bool = True,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        refresh_buffer_seconds: int = DEFAULT_REFRESH_BUFFER_SECONDS,
    ) -> None:
        """
        Initialize Moata OAuth2 authentication.
        
        Args:
            token_url: OAuth2 token endpoint URL
            scope: OAuth2 scope for API access
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            verify_ssl: Whether to verify SSL certificates (default: True)
            timeout_seconds: Request timeout in seconds (default: 30)
            ttl_seconds: Token time-to-live in seconds (default: 3600)
            refresh_buffer_seconds: Refresh buffer in seconds (default: 300)
            
        Raises:
            ValueError: If required parameters are empty
        """
        # Validate required parameters
        if not token_url:
            raise ValueError("token_url cannot be empty")
        if not scope:
            raise ValueError("scope cannot be empty")
        if not client_id:
            raise ValueError("client_id cannot be empty")
        if not client_secret:
            raise ValueError("client_secret cannot be empty")
        
        self._token_url = token_url
        self._scope = scope
        self._client_id = client_id
        self._client_secret = client_secret
        self._verify_ssl = verify_ssl
        self._timeout = timeout_seconds
        self._ttl = ttl_seconds
        self._buffer = refresh_buffer_seconds
        self._token: Optional[Token] = None
        
        # Setup logger
        self._logger = logging.getLogger(__name__)
        
        # Log initialization (without sensitive data)
        self._logger.debug(
            f"MoataAuth initialized: url={token_url}, "
            f"verify_ssl={verify_ssl}, timeout={timeout_seconds}s"
        )

    def get_token(self) -> str:
        """
        Get valid access token (fetches new if needed).
        
        Automatically refreshes token if:
        - No token cached
        - Token is expired
        - Token expires within refresh buffer
        
        Returns:
            Valid access token string
            
        Raises:
            AuthenticationError: If token acquisition fails
            TokenRefreshError: If token refresh fails
            
        Example:
            >>> token = auth.get_token()
            >>> headers = {"Authorization": f"Bearer {token}"}
        """
        if self._token is None:
            self._logger.info("No cached token, acquiring new token")
            self._token = self._request_token()
        elif self._token.is_expired():
            self._logger.warning("Token expired, acquiring new token")
            self._token = self._request_token()
        elif self._token.near_expiry(self._buffer):
            self._logger.info(
                f"Token expires in {self._token.expires_in():.0f}s, refreshing"
            )
            self._token = self._request_token()
        else:
            self._logger.debug(
                f"Using cached token (expires in {self._token.expires_in():.0f}s)"
            )
        
        return self._token.access_token

    def _request_token(self) -> Token:
        """
        Request new access token from OAuth2 endpoint.
        
        Implements retry logic with exponential backoff for network resilience.
        
        Returns:
            New Token instance
            
        Raises:
            AuthenticationError: If authentication fails after retries
        """
        self._logger.info("Requesting access token...")
        
        # Prepare request data (never log client_secret!)
        data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": "client_credentials",
        }
        params = {"scope": self._scope}
        
        # Create session with retry logic
        session = self._create_retry_session()
        
        try:
            resp = self._make_token_request(session, data, params)
            
        except requests.exceptions.ConnectionError as e:
            # Connection error - try one more time after brief delay
            self._logger.warning(
                f"Token request connection error: {e}. "
                f"Retrying after {CONNECTION_ERROR_RETRY_DELAY}s..."
            )
            time.sleep(CONNECTION_ERROR_RETRY_DELAY)
            
            try:
                resp = self._make_token_request(session, data, params)
            except Exception as retry_error:
                raise AuthenticationError(
                    f"Failed to acquire token after retry: {retry_error}"
                ) from retry_error
                
        except requests.exceptions.Timeout as e:
            raise AuthenticationError(
                f"Token request timed out after {self._timeout}s: {e}"
            ) from e
            
        except requests.exceptions.RequestException as e:
            raise AuthenticationError(
                f"Token request failed: {e}"
            ) from e
        
        # Validate response
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Try to get error details from response
            try:
                error_detail = resp.json()
                raise AuthenticationError(
                    f"Authentication failed (HTTP {resp.status_code}): {error_detail}"
                ) from e
            except ValueError:
                raise AuthenticationError(
                    f"Authentication failed (HTTP {resp.status_code}): {resp.text}"
                ) from e
        
        # Parse response
        try:
            payload = resp.json()
        except ValueError as e:
            raise AuthenticationError(
                f"Invalid JSON response from token endpoint: {resp.text}"
            ) from e
        
        # Extract token
        token = payload.get("access_token")
        if not token:
            raise AuthenticationError(
                f"No access_token in response. Available fields: {list(payload.keys())}"
            )
        
        self._logger.info("✓ Successfully obtained access token")
        self._logger.debug(f"Token length: {len(token)} characters")
        
        return Token(
            access_token=token,
            acquired_at=time.time(),
            ttl_seconds=self._ttl
        )

    def _create_retry_session(self) -> requests.Session:
        """
        Create requests session with retry logic.
        
        Returns:
            Configured requests.Session with retry adapter
        """
        session = requests.Session()
        
        retries = Retry(
            total=DEFAULT_RETRY_ATTEMPTS,
            connect=DEFAULT_RETRY_ATTEMPTS,
            read=DEFAULT_RETRY_ATTEMPTS,
            backoff_factor=DEFAULT_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods={"POST"},
            raise_on_status=False,
        )
        
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session

    def _make_token_request(
        self,
        session: requests.Session,
        data: dict,
        params: dict
    ) -> requests.Response:
        """
        Make token request to OAuth2 endpoint.
        
        Args:
            session: Requests session with retry logic
            data: Request body data
            params: Query parameters
            
        Returns:
            Response from token endpoint
        """
        return session.post(
            self._token_url,
            data=data,
            params=params,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )

    def clear_token(self) -> None:
        """
        Clear cached token (force refresh on next get_token call).
        
        Useful for testing or when token is known to be invalid.
        """
        self._logger.debug("Clearing cached token")
        self._token = None

    def get_token_info(self) -> dict:
        """
        Get information about current token.
        
        Returns:
            Dictionary with token info (no sensitive data)
            
        Example:
            >>> info = auth.get_token_info()
            >>> print(info)
            {'cached': True, 'expires_in': 3245.2, 'near_expiry': False}
        """
        if self._token is None:
            return {
                "cached": False,
                "expires_in": None,
                "near_expiry": None,
                "is_expired": None,
            }
        
        return {
            "cached": True,
            "expires_in": self._token.expires_in(),
            "near_expiry": self._token.near_expiry(self._buffer),
            "is_expired": self._token.is_expired(),
        }