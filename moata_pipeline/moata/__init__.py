"""
Moata API Client Package

Provides OAuth2 authentication, HTTP client, and high-level API client
for the Moata API.

Usage:
    from moata_pipeline.moata import MoataAuth, MoataHttp, MoataClient
    
    # Initialize authentication
    auth = MoataAuth(
        token_url="https://api.moata.io/oauth2/token",
        scope="https://moata.onmicrosoft.com/moata.io/.default",
        client_id="your_client_id",
        client_secret="your_secret",
        verify_ssl=True
    )
    
    # Initialize HTTP client
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url="https://api.moata.io",
        requests_per_second=2.0,
        verify_ssl=True
    )
    
    # Initialize API client
    client = MoataClient(http=http)
    
    # Use the client
    gauges = client.get_rain_gauges(project_id=594, asset_type_id=25)

Modules:
    - auth: OAuth2 authentication with token caching
    - http: HTTP client with rate limiting and retries
    - client: High-level API client with domain methods
    - endpoints: API endpoint path definitions

Classes:
    - MoataAuth: OAuth2 authentication manager
    - MoataHttp: HTTP client with rate limiting
    - MoataClient: High-level API client
    - Token: Access token with expiry tracking

Exceptions:
    - AuthenticationError: Authentication failures
    - TokenRefreshError: Token refresh failures
    - HTTPError: HTTP request failures
    - RateLimitError: Rate limit exceeded
    - TimeoutError: Request timeout
    - ValidationError: Parameter validation failures

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

# Version info
__version__ = "1.0.0"
__author__ = "Auckland Council Internship Team"
__email__ = "mott909@aucklanduni.ac.nz"

# Import main classes for convenient access
from .auth import MoataAuth, Token, AuthenticationError, TokenRefreshError
from .http import (
    MoataHttp,
    HTTPError,
    RateLimitError,
    TimeoutError,
    AuthenticationError as HttpAuthError
)
from .client import MoataClient, ValidationError

# Import endpoints module (typically used with alias)
from . import endpoints

# Define public API
__all__ = [
    # Main classes
    "MoataAuth",
    "MoataHttp",
    "MoataClient",
    "Token",
    
    # Exceptions
    "AuthenticationError",
    "TokenRefreshError",
    "HTTPError",
    "RateLimitError",
    "TimeoutError",
    "ValidationError",
    
    # Modules
    "endpoints",
    
    # Metadata
    "__version__",
    "__author__",
]


def get_version() -> str:
    """
    Get package version string.
    
    Returns:
        Version string (e.g., "1.0.0")
        
    Example:
        >>> from moata_pipeline.moata import get_version
        >>> print(get_version())
        1.0.0
    """
    return __version__


def create_client(
    client_id: str,
    client_secret: str,
    token_url: str = "https://api.moata.io/oauth2/token",
    base_url: str = "https://api.moata.io",
    scope: str = "https://moata.onmicrosoft.com/moata.io/.default",
    verify_ssl: bool = True,
    requests_per_second: float = 2.0,
) -> MoataClient:
    """
    Create a fully configured Moata API client (convenience function).
    
    Args:
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        token_url: OAuth2 token endpoint URL
        base_url: Moata API base URL
        scope: OAuth2 scope
        verify_ssl: Whether to verify SSL certificates
        requests_per_second: Rate limit (requests per second)
        
    Returns:
        Configured MoataClient instance
        
    Raises:
        ValueError: If required parameters are missing
        AuthenticationError: If authentication fails
        
    Example:
        >>> from moata_pipeline.moata import create_client
        >>> client = create_client(
        ...     client_id="your_id",
        ...     client_secret="your_secret"
        ... )
        >>> gauges = client.get_rain_gauges(594, 25)
    """
    # Create auth
    auth = MoataAuth(
        token_url=token_url,
        scope=scope,
        client_id=client_id,
        client_secret=client_secret,
        verify_ssl=verify_ssl,
    )
    
    # Create HTTP client
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=base_url,
        requests_per_second=requests_per_second,
        verify_ssl=verify_ssl,
    )
    
    # Create and return client
    return MoataClient(http=http)