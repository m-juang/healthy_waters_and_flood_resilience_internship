"""
Moata API Endpoint Definitions

Contains all API endpoint path templates used by the Moata client.
Endpoints use Python format string placeholders for dynamic values.

Usage:
    from moata_pipeline.moata import endpoints as ep
    
    # Format endpoint with project_id
    path = ep.PROJECT_ASSETS.format(project_id=594)
    # Result: "projects/594/assets"

Notes:
    - All endpoints are relative paths (no leading slash)
    - Base URL (e.g., "https://api.moata.io") is configured in HTTP client
    - Version prefix "/v1/" is typically included in base URL
    - Use .format() to substitute placeholders like {project_id}

API Documentation:
    Full API documentation available at Moata API portal.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

# Type annotation for all endpoints
from typing import Final

# ============================================================================
# PROJECTS & ASSETS
# ============================================================================

PROJECT_ASSETS: Final[str] = "projects/{project_id}/assets"
"""
Get assets for a project.

Placeholders:
    - project_id: Moata project ID (int)

Example:
    >>> path = PROJECT_ASSETS.format(project_id=594)
    >>> # "projects/594/assets"
"""

# ============================================================================
# TRACES (TIMESERIES METADATA)
# ============================================================================

ASSET_TRACES: Final[str] = "assets/traces"
"""
Get traces (timeseries metadata) for assets.

Query params typically include:
    - assetId: Asset ID(s) to fetch traces for

Example:
    >>> path = ASSET_TRACES
    >>> # "assets/traces?assetId=12345"
"""

TRACE_THRESHOLDS: Final[str] = "traces/{trace_id}/thresholds"
"""
Get alarm thresholds configured for a trace.

Placeholders:
    - trace_id: Trace ID (int)

Example:
    >>> path = TRACE_THRESHOLDS.format(trace_id=12345)
    >>> # "traces/12345/thresholds"
"""

TRACE_DATA_UTC: Final[str] = "traces/{trace_id}/data/utc"
"""
Get timeseries data for a trace (UTC timestamps).

Placeholders:
    - trace_id: Trace ID (int)

Query params typically include:
    - from: Start time (ISO 8601)
    - to: End time (ISO 8601)
    - dataType: Data type (e.g., "None", "Aggregated")
    - dataInterval: Interval in seconds (optional)

Example:
    >>> path = TRACE_DATA_UTC.format(trace_id=12345)
    >>> # "traces/12345/data/utc?from=2025-01-01T00:00:00Z&to=..."
"""

TRACE_ARI: Final[str] = "traces/{trace_id}/ari"
"""
Get ARI (Annual Recurrence Interval) data for a trace.

Placeholders:
    - trace_id: Trace ID (int)

Query params typically include:
    - from: Start time (ISO 8601)
    - to: End time (ISO 8601)
    - type: ARI calculation type (e.g., "Tp108")

Example:
    >>> path = TRACE_ARI.format(trace_id=12345)
    >>> # "traces/12345/ari?from=...&to=...&type=Tp108"
"""

# ============================================================================
# ALARMS
# ============================================================================

ALARMS_OVERFLOW_BY_TRACE: Final[str] = "alarms/overflow-detailed-info-by-trace"
"""
Get overflow alarm details for a trace.

Query params typically include:
    - traceId: Trace ID (int)

Returns:
    List of AlarmDetailedInfoDto

Example:
    >>> path = ALARMS_OVERFLOW_BY_TRACE
    >>> # "alarms/overflow-detailed-info-by-trace?traceId=12345"
"""

ALARMS_DETAILED_BY_PROJECT: Final[str] = "alarms/detailed-alarms"
"""
Get detailed alarms for all traces in a project.

Query params typically include:
    - projectId: Project ID (int)

Returns:
    List of AlarmDetailedInfoDto

Example:
    >>> path = ALARMS_DETAILED_BY_PROJECT
    >>> # "alarms/detailed-alarms?projectId=594"
"""

# ============================================================================
# RADAR / TRACESET COLLECTIONS (QPE)
# ============================================================================

TRACESET_COLLECTION_DATA: Final[str] = "trace-set-collections/{collection_id}/trace-sets/data"
"""
Get radar data values for specified tracesets and pixels.

Placeholders:
    - collection_id: TraceSet collection ID (int, e.g., 1 for QPE)

Query params typically include:
    - TsId: List of traceset IDs (e.g., [3] for QPE)
    - Pi: List of pixel indices (max 150 per request)
    - StartTime: Start time (ISO 8601)
    - EndTime: End time (ISO 8601)

Returns:
    List of TraceSetDataValuesDto

Example:
    >>> path = TRACESET_COLLECTION_DATA.format(collection_id=1)
    >>> # "trace-set-collections/1/trace-sets/data?TsId=3&Pi=100&Pi=101&..."
"""

TRACESET_PIXEL_MAPPINGS: Final[str] = "trace-set-collections/{collection_id}/pixel-mappings/intersects-geometry"
"""
Get pixel indices that intersect with a geometry.

Placeholders:
    - collection_id: TraceSet collection ID (int, e.g., 1 for QPE)

Query params typically include:
    - wkt: Well-Known Text geometry string
    - srId: Spatial Reference ID (e.g., 4326 for WGS84)

Returns:
    List of TraceSetPixelMappingDto

Example:
    >>> path = TRACESET_PIXEL_MAPPINGS.format(collection_id=1)
    >>> # "trace-set-collections/1/pixel-mappings/intersects-geometry?wkt=POLYGON(...)&srId=4326"
"""

# ============================================================================
# ENDPOINT REGISTRY
# ============================================================================

# All available endpoints for validation/documentation
ALL_ENDPOINTS: Final[dict[str, str]] = {
    "PROJECT_ASSETS": PROJECT_ASSETS,
    "ASSET_TRACES": ASSET_TRACES,
    "TRACE_THRESHOLDS": TRACE_THRESHOLDS,
    "TRACE_DATA_UTC": TRACE_DATA_UTC,
    "TRACE_ARI": TRACE_ARI,
    "ALARMS_OVERFLOW_BY_TRACE": ALARMS_OVERFLOW_BY_TRACE,
    "ALARMS_DETAILED_BY_PROJECT": ALARMS_DETAILED_BY_PROJECT,
    "TRACESET_COLLECTION_DATA": TRACESET_COLLECTION_DATA,
    "TRACESET_PIXEL_MAPPINGS": TRACESET_PIXEL_MAPPINGS,
}
"""
Registry of all endpoint definitions.

Useful for:
- Validation
- Documentation generation
- Testing

Example:
    >>> from moata_pipeline.moata.endpoints import ALL_ENDPOINTS
    >>> for name, path in ALL_ENDPOINTS.items():
    ...     print(f"{name}: {path}")
"""


def get_endpoint_placeholders(endpoint: str) -> list[str]:
    """
    Extract placeholder names from an endpoint string.
    
    Args:
        endpoint: Endpoint path string
        
    Returns:
        List of placeholder names (e.g., ["project_id", "trace_id"])
        
    Example:
        >>> get_endpoint_placeholders(PROJECT_ASSETS)
        ['project_id']
        >>> get_endpoint_placeholders(TRACE_DATA_UTC)
        ['trace_id']
    """
    import re
    return re.findall(r'\{(\w+)\}', endpoint)


def validate_endpoint_format(endpoint: str, **kwargs) -> str:
    """
    Validate and format an endpoint with provided values.
    
    Args:
        endpoint: Endpoint path string
        **kwargs: Values for placeholders
        
    Returns:
        Formatted endpoint string
        
    Raises:
        KeyError: If required placeholder is missing
        ValueError: If endpoint is invalid
        
    Example:
        >>> validate_endpoint_format(PROJECT_ASSETS, project_id=594)
        'projects/594/assets'
        >>> validate_endpoint_format(TRACE_DATA_UTC, trace_id=12345)
        'traces/12345/data/utc'
    """
    if not endpoint:
        raise ValueError("endpoint cannot be empty")
    
    placeholders = get_endpoint_placeholders(endpoint)
    
    # Check all required placeholders are provided
    missing = [p for p in placeholders if p not in kwargs]
    if missing:
        raise KeyError(
            f"Missing required placeholders: {missing}. "
            f"Endpoint '{endpoint}' requires: {placeholders}"
        )
    
    # Format endpoint
    try:
        return endpoint.format(**kwargs)
    except KeyError as e:
        raise KeyError(
            f"Invalid placeholder in endpoint '{endpoint}': {e}"
        ) from e