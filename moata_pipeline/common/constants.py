"""
Configuration Constants for Moata Pipeline

Contains all configuration constants for the Auckland Council rain monitoring
data pipeline. These values are used throughout the application for API
connections, data processing, and filtering.

Categories:
    - Moata API Endpoints
    - Project & Asset IDs
    - Rate Limiting & Timeouts
    - Token Management
    - Data Filtering
    - Radar/TraceSet Configuration
    - File Paths

Usage:
    from moata_pipeline.common.constants import (
        TOKEN_URL,
        BASE_API_URL,
        PROJECT_ID,
        RAIN_GAUGE_ASSET_TYPE_ID
    )

Environment Variables:
    Some constants can be overridden via environment variables:
    - MOATA_CLIENT_ID (required)
    - MOATA_CLIENT_SECRET (required)
    - LOG_LEVEL (optional)

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

from typing import Final

# ============================================================================
# MOATA API ENDPOINTS
# ============================================================================

TOKEN_URL: Final[str] = (
    "https://moata.b2clogin.com/"
    "moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token"
)
"""
OAuth2 token endpoint URL for Moata API authentication.

This endpoint is used to obtain access tokens via client credentials flow.
"""

BASE_API_URL: Final[str] = "https://api.moata.io/ae/v1"
"""
Base URL for Moata API (includes version prefix /v1).

All API endpoints are relative to this base URL.
Example: {BASE_API_URL}/projects/594/assets
"""

OAUTH_SCOPE: Final[str] = "https://moata.onmicrosoft.com/moata.io/.default"
"""
OAuth2 scope for Moata API access.

Required scope for client credentials authentication.
"""

# ============================================================================
# PROJECT & ASSET TYPE IDS
# ============================================================================

PROJECT_ID: Final[int] = 594
"""
Default Moata project ID for Auckland Council.

This is the main project containing rain gauges and radar data.
"""

# Legacy constant for backwards compatibility
DEFAULT_PROJECT_ID: Final[int] = PROJECT_ID
"""
Deprecated: Use PROJECT_ID instead.

Maintained for backwards compatibility with older scripts.
"""

RAIN_GAUGE_ASSET_TYPE_ID: Final[int] = 100
"""
Asset type ID for rain gauge assets in Moata.

Used to filter assets when retrieving rain gauge data.
"""

# Legacy constant for backwards compatibility
DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID: Final[int] = RAIN_GAUGE_ASSET_TYPE_ID
"""
Deprecated: Use RAIN_GAUGE_ASSET_TYPE_ID instead.

Maintained for backwards compatibility with older scripts.
"""

STORMWATER_CATCHMENT_ASSET_TYPE_ID: Final[int] = 3541
"""
Asset type ID for stormwater catchment assets in Moata.

Used to filter catchment polygons for radar analysis.
"""

# ============================================================================
# RATE LIMITING & TIMEOUTS
# ============================================================================

DEFAULT_REQUESTS_PER_SECOND: Final[float] = 2.0
"""
Default rate limit for Moata API requests.

Based on Sam's guidance: 800 requests per 5 minutes.
Calculation: 800 requests / 300 seconds = 2.67 requests/second
Using 2.0 as safe default with buffer.
"""

DEFAULT_TIMEOUT_SECONDS: Final[int] = 60
"""
Default read timeout for HTTP requests in seconds.

Applies to API calls when fetching data. Connection timeout
is typically lower (see connect_timeout_seconds in http.py).
"""

# ============================================================================
# TOKEN MANAGEMENT
# ============================================================================

TOKEN_TTL_SECONDS: Final[int] = 3600
"""
Token Time-To-Live in seconds (1 hour).

Access tokens from Moata API are valid for this duration.
"""

TOKEN_REFRESH_BUFFER_SECONDS: Final[int] = 300
"""
Token refresh buffer in seconds (5 minutes).

Tokens are refreshed this many seconds before expiry to avoid
authentication errors due to timing issues.
"""

# ============================================================================
# DATA FILTERING - RAIN GAUGES
# ============================================================================

INACTIVE_THRESHOLD_MONTHS: Final[int] = 3
"""
Threshold for marking rain gauges as inactive.

Gauges with no data in the last N months are considered inactive.
Default: 3 months
"""

DEFAULT_EXCLUDE_KEYWORD: Final[str] = "northland|waikato"
"""
Default keyword pattern for excluding rain gauges.

Pipe-separated regex pattern for filtering out gauges by name.
Examples: "test", "northland|waikato", "backup|offline"
"""

# ============================================================================
# RADAR / TRACESET CONFIGURATION
# ============================================================================

RADAR_COLLECTION_ID: Final[int] = 1
"""
TraceSet collection ID for radar QPE (Quantitative Precipitation Estimate).

Used for fetching radar rainfall data.
"""

RADAR_QPE_TRACESET_ID: Final[int] = 3
"""
TraceSet ID for radar QPE data within the collection.

Used when requesting specific radar datasets.
"""

RADAR_MAX_PIXELS_PER_REQUEST: Final[int] = 150
"""
Maximum number of radar pixels per API request.

API limitation enforced by Moata. Requests exceeding this
limit will be batched automatically.
"""

RADAR_RECOMMENDED_BATCH_SIZE: Final[int] = 50
"""
Recommended batch size for radar pixel requests.

Optimal balance between request count and API performance.
Use this instead of maximum (150) for better reliability.
"""

# ============================================================================
# ARI (ANNUAL RECURRENCE INTERVAL) CONFIGURATION
# ============================================================================

DEFAULT_ARI_THRESHOLD: Final[float] = 5.0
"""
Default ARI threshold in years for alarm validation.

Alarms are expected to trigger when ARI exceeds this value.
Common values: 2.0, 5.0, 10.0, 20.0, 50.0, 100.0
"""

DEFAULT_ARI_TYPE: Final[str] = "Tp108"
"""
Default ARI calculation type.

"Tp108" refers to the duration used in ARI calculations.
Consult Moata documentation for available types.
"""

DEFAULT_ARI_TRACE_DESCRIPTION: Final[str] = "Max TP108 ARI"
"""
Default trace description for ARI traces.

Used to identify ARI-related traces in the API.
"""

# ============================================================================
# TIME WINDOWS
# ============================================================================

DEFAULT_VALIDATION_WINDOW_HOURS_BEFORE: Final[int] = 1
"""
Default time window before alarm for validation (hours).

When validating alarms, check data from N hours before alarm time.
"""

DEFAULT_VALIDATION_WINDOW_HOURS_AFTER: Final[int] = 1
"""
Default time window after alarm for validation (hours).

When validating alarms, check data up to N hours after alarm time.
"""

DEFAULT_DATA_INTERVAL_SECONDS: Final[int] = 300
"""
Default data interval for timeseries requests (5 minutes).

Used when fetching trace data from Moata API.
"""

# ============================================================================
# FILE PATHS & DIRECTORIES
# ============================================================================

DEFAULT_OUTPUT_BASE_DIR: Final[str] = "outputs"
"""
Base directory for all output files.

All generated files (CSVs, dashboards, reports) are saved here.
"""

DEFAULT_DATA_INPUT_DIR: Final[str] = "data/inputs"
"""
Default directory for input data files.

Location for uploaded CSVs, configuration files, etc.
"""

# Rain Gauge paths
RAIN_GAUGE_OUTPUT_DIR: Final[str] = "outputs/rain_gauges"
RAIN_GAUGE_ANALYZE_DIR: Final[str] = "outputs/rain_gauges/analyze"
RAIN_GAUGE_VALIDATION_DIR: Final[str] = "outputs/rain_gauges/validation_viz"

# Rain Radar paths
RAIN_RADAR_OUTPUT_DIR: Final[str] = "outputs/rain_radar"
RAIN_RADAR_ANALYZE_DIR: Final[str] = "outputs/rain_radar/analyze"
RAIN_RADAR_VALIDATION_DIR: Final[str] = "outputs/rain_radar/validation_viz"
RAIN_RADAR_HISTORICAL_DIR: Final[str] = "outputs/rain_radar/historical"

# ============================================================================
# DATA TYPES
# ============================================================================

DEFAULT_DATA_TYPE: Final[str] = "None"
"""
Default data type for trace data requests.

"None" returns raw data. Other options: "Aggregated", "Interpolated"
Consult Moata API documentation for available types.
"""

DEFAULT_PAD_WITH_ZEROES: Final[bool] = False
"""
Default setting for padding missing data with zeroes.

If True, missing data points are filled with 0 values.
If False, missing data points are omitted.
"""

# ============================================================================
# SPATIAL REFERENCE
# ============================================================================

DEFAULT_SR_ID: Final[int] = 4326
"""
Default Spatial Reference ID (WGS84).

Used for geometry coordinates in API requests.
4326 = WGS84 (standard lat/lon coordinates)
"""

# ============================================================================
# LOGGING
# ============================================================================

DEFAULT_LOG_LEVEL: Final[str] = "INFO"
"""
Default logging level for the application.

Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
Can be overridden via --log-level CLI argument or LOG_LEVEL env var.
"""

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_all_constants() -> dict:
    """
    Get all constants as a dictionary.
    
    Returns:
        Dictionary mapping constant names to values
        
    Example:
        >>> from moata_pipeline.common.constants import get_all_constants
        >>> constants = get_all_constants()
        >>> print(constants['PROJECT_ID'])
        594
    """
    import sys
    current_module = sys.modules[__name__]
    
    # Get all uppercase attributes (constants)
    return {
        name: getattr(current_module, name)
        for name in dir(current_module)
        if name.isupper() and not name.startswith('_')
    }


def print_constants() -> None:
    """
    Print all constants in a formatted way.
    
    Useful for debugging and configuration verification.
    
    Example:
        >>> from moata_pipeline.common.constants import print_constants
        >>> print_constants()
    """
    constants = get_all_constants()
    
    print("=" * 80)
    print("MOATA PIPELINE CONFIGURATION CONSTANTS")
    print("=" * 80)
    
    for name, value in sorted(constants.items()):
        # Format value
        if isinstance(value, str) and len(value) > 60:
            value_str = f"{value[:57]}..."
        else:
            value_str = repr(value)
        
        print(f"{name:40s} = {value_str}")
    
    print("=" * 80)


def validate_constants() -> bool:
    """
    Validate that all required constants are properly set.
    
    Returns:
        True if all constants are valid, False otherwise
        
    Raises:
        ValueError: If critical constants are missing or invalid
        
    Example:
        >>> from moata_pipeline.common.constants import validate_constants
        >>> if validate_constants():
        ...     print("Configuration valid!")
    """
    errors = []
    
    # Check URLs
    if not TOKEN_URL or not TOKEN_URL.startswith("https://"):
        errors.append("TOKEN_URL must be a valid HTTPS URL")
    
    if not BASE_API_URL or not BASE_API_URL.startswith("https://"):
        errors.append("BASE_API_URL must be a valid HTTPS URL")
    
    # Check IDs
    if PROJECT_ID <= 0:
        errors.append("PROJECT_ID must be positive")
    
    if RAIN_GAUGE_ASSET_TYPE_ID <= 0:
        errors.append("RAIN_GAUGE_ASSET_TYPE_ID must be positive")
    
    # Check rate limits
    if DEFAULT_REQUESTS_PER_SECOND <= 0:
        errors.append("DEFAULT_REQUESTS_PER_SECOND must be positive")
    
    # Check timeouts
    if DEFAULT_TIMEOUT_SECONDS <= 0:
        errors.append("DEFAULT_TIMEOUT_SECONDS must be positive")
    
    if TOKEN_TTL_SECONDS <= 0:
        errors.append("TOKEN_TTL_SECONDS must be positive")
    
    if TOKEN_REFRESH_BUFFER_SECONDS < 0:
        errors.append("TOKEN_REFRESH_BUFFER_SECONDS cannot be negative")
    
    if errors:
        raise ValueError(
            "Configuration validation failed:\n" +
            "\n".join(f"  - {e}" for e in errors)
        )
    
    return True