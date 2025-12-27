"""
Moata Data Pipeline Package

A comprehensive data pipeline for Auckland Council's rain monitoring system.
Collects, analyzes, and visualizes data from the Moata API for rain gauges
and radar (QPE) datasets.

Pipeline Stages:
    - collect: Fetch data from Moata API (gauges, radar, traces)
    - analyze: Offline filtering, analysis, and alarm validation
    - viz: Reporting and visualization (HTML dashboards, charts)

Modules:
    - moata: API client (auth, http, client, endpoints)
    - collect: Data collection runners and collectors
    - analyze: Analysis runners and algorithms
    - viz: Visualization runners and report generators
    - common: Shared utilities (constants, file I/O, time utils)
    - logging_setup: Centralized logging configuration

Entry Point Scripts:
    Rain Gauges:
        - retrieve_rain_gauges.py: Collect gauge metadata
        - analyze_rain_gauges.py: Analyze gauge activity and traces
        - visualize_rain_gauges.py: Create gauge analysis dashboard
        - validate_ari_alarms_rain_gauges.py: Validate ARI alarm events
        - visualize_ari_alarms_rain_gauges.py: Create validation dashboard
    
    Rain Radar:
        - retrieve_rain_radar.py: Collect radar data for catchments
        - analyze_rain_radar.py: Analyze radar ARI exceedances
        - visualize_rain_radar.py: Create radar analysis dashboard
        - validate_ari_alarms_rain_radar.py: Validate radar alarms
        - visualize_ari_alarms_rain_radar.py: Create radar validation dashboard

Usage:
    # Import API client
    from moata_pipeline.moata import MoataAuth, MoataHttp, MoataClient
    
    # Import utilities
    from moata_pipeline.logging_setup import setup_logging
    from moata_pipeline.common.constants import PROJECT_ID, BASE_API_URL
    
    # Setup logging
    setup_logging("INFO")
    
    # Create client
    auth = MoataAuth(...)
    http = MoataHttp(get_token_fn=auth.get_token, ...)
    client = MoataClient(http=http)

Configuration:
    Environment variables (in .env):
        - MOATA_CLIENT_ID: OAuth2 client ID (required)
        - MOATA_CLIENT_SECRET: OAuth2 client secret (required)
        - LOG_LEVEL: Logging level (optional, default: INFO)

Project Structure:
    moata_pipeline/
    ├── __init__.py              # This file
    ├── logging_setup.py         # Logging configuration
    │
    ├── moata/                   # API client package
    │   ├── __init__.py
    │   ├── auth.py              # OAuth2 authentication
    │   ├── http.py              # HTTP client with rate limiting
    │   ├── client.py            # High-level API client
    │   └── endpoints.py         # API endpoint definitions
    │
    ├── collect/                 # Data collection
    │   ├── __init__.py
    │   ├── collector.py         # Data collectors
    │   └── runner.py            # Collection runners
    │
    ├── analyze/                 # Data analysis
    │   ├── __init__.py
    │   ├── alarm_analysis.py    # Alarm analysis
    │   ├── ari_calculator.py    # ARI calculations
    │   ├── filtering.py         # Data filtering
    │   ├── radar_analysis.py    # Radar analysis
    │   ├── reporting.py         # Report generation
    │   └── runner.py            # Analysis runners
    │
    ├── viz/                     # Visualization
    │   ├── __init__.py
    │   ├── cleaning.py          # Data cleaning for viz
    │   ├── pages.py             # HTML page generation
    │   ├── radar_cleaning.py    # Radar data cleaning
    │   ├── radar_report.py      # Radar reports
    │   ├── radar_runner.py      # Radar viz runners
    │   ├── report.py            # Gauge reports
    │   └── runner.py            # Gauge viz runners
    │
    └── common/                  # Shared utilities
        ├── __init__.py
        ├── constants.py         # Configuration constants
        ├── dataframe_utils.py   # Pandas utilities
        ├── file_utils.py        # File I/O utilities
        ├── html_utils.py        # HTML generation
        ├── iter_utils.py        # Iterator utilities
        ├── json_io.py           # JSON I/O
        ├── output_writer.py     # Output file writing
        ├── paths.py             # Path utilities
        ├── text_utils.py        # Text utilities
        ├── time_utils.py        # Time/date utilities
        └── typing_utils.py      # Type hints

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

# Package metadata
__version__ = "1.0.0"
__author__ = "Auckland Council Internship Team"
__email__ = "mott909@aucklanduni.ac.nz"
__description__ = "Data pipeline for Auckland Council rain monitoring system"
__url__ = "https://github.com/m-juang/healthy_waters_and_flood_resilience_internship"  

# Import commonly used components for convenience
from .logging_setup import setup_logging, get_logger

# Version info function
def get_version() -> str:
    """
    Get package version string.
    
    Returns:
        Version string (e.g., "1.0.0")
        
    Example:
        >>> from moata_pipeline import get_version
        >>> print(get_version())
        1.0.0
    """
    return __version__


def get_package_info() -> dict:
    """
    Get package metadata.
    
    Returns:
        Dictionary with package metadata
        
    Example:
        >>> from moata_pipeline import get_package_info
        >>> info = get_package_info()
        >>> print(info['version'], info['author'])
    """
    return {
        "name": "moata_pipeline",
        "version": __version__,
        "author": __author__,
        "email": __email__,
        "description": __description__,
        "url": __url__,
    }


# Define public API
__all__ = [
    # Logging
    "setup_logging",
    "get_logger",
    
    # Metadata
    "__version__",
    "__author__",
    "__description__",
    "get_version",
    "get_package_info",
]