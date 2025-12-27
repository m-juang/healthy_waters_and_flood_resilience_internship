"""
Data Collection Package

Provides collectors and runners for fetching data from Moata API.

Collectors:
    RainGaugeCollector: Collects rain gauge assets, traces, alarms, and thresholds
    RadarDataCollector: Collects radar QPE data for stormwater catchments

Runners:
    run_collect_rain_gauges: High-level entry point for rain gauge collection
    run_collect_radar: High-level entry point for radar collection

Example:
    >>> from moata_pipeline.collect import RainGaugeCollector, run_collect_rain_gauges
    >>> from moata_pipeline.moata import create_client
    >>> 
    >>> # Option 1: Use high-level runner
    >>> run_collect_rain_gauges(project_id=594, asset_type_id=100)
    >>> 
    >>> # Option 2: Use collector directly
    >>> client = create_client(client_id="...", client_secret="...")
    >>> collector = RainGaugeCollector(client)
    >>> data = collector.collect(project_id=594, asset_type_id=100)

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from moata_pipeline.collect.collector import (
    RainGaugeCollector,
    RadarDataCollector,
    CollectionError,
    GeometryError,
    CacheError,
)

from moata_pipeline.collect.runner import (
    run_collect_rain_gauges,
    run_collect_radar,
    CollectionRunnerError,
    CredentialsError,
    ClientCreationError,
)

# Version info
__version__ = "1.0.0"
__author__ = "Auckland Council Internship Team (COMPSCI 778)"

# Public API
__all__ = [
    # Collectors
    "RainGaugeCollector",
    "RadarDataCollector",
    
    # Runners
    "run_collect_rain_gauges",
    "run_collect_radar",
    
    # Exceptions - Collector
    "CollectionError",
    "GeometryError",
    "CacheError",
    
    # Exceptions - Runner
    "CollectionRunnerError",
    "CredentialsError",
    "ClientCreationError",
    
    # Metadata
    "__version__",
    "__author__",
]