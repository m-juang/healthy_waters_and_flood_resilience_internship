"""
Specialized collector components following Single Responsibility Principle.

This module contains focused collector classes that each handle one specific
aspect of data collection, replacing the monolithic collector.py approach.

Architecture:
-----------
OLD (collector.py - 1995 lines):
    RainGaugeCollector: Fetch gauges, traces, alarms, thresholds, save files
    RadarDataCollector: Fetch catchments, pixels, radar data, save files

NEW (collectors/ - ~500 lines total):
    AssetFetcher: Fetch and prepare asset data (SRP)
    TraceFetcher: Fetch trace and timeseries data (SRP)
    AlarmFetcher: Fetch alarm and threshold data (SRP)
    OutputManager: Handle file I/O and atomic writes (SRP)
    GaugeCollector: Orchestrate gauge collection workflow (Facade)
    RadarCollector: Orchestrate radar collection workflow (Facade - TODO)

Benefits:
--------
✅ Single Responsibility Principle - Each class has one job
✅ Easier testing - Mock individual components
✅ Better reusability - Use AssetFetcher independently
✅ Clearer code - Explicit dependencies
✅ Backwards compatible - Facades maintain old interface

Usage:
-----
# NEW STYLE (Recommended):
>>> from moata_pipeline.collect.collectors import GaugeCollector
>>> collector = GaugeCollector(client)
>>> data = collector.collect(594, 25)

# DIRECT USAGE (Advanced):
>>> from moata_pipeline.collect.collectors import AssetFetcher, TraceFetcher
>>> assets = AssetFetcher(client)
>>> traces = TraceFetcher(client)
>>> gauges = assets.fetch_gauges(594, 25)
>>> trace_data = traces.fetch_traces_batched(asset_ids)

# OLD STYLE (Still works via collector.py):
>>> from moata_pipeline.collect.collector import RainGaugeCollector
>>> collector = RainGaugeCollector(client)  # Delegates to new architecture
>>> data = collector.collect(594, 25)

Classes:
-------
    BaseCollector: Base class with common functionality
    AssetFetcher: Fetch and prepare asset data
    TraceFetcher: Fetch trace and timeseries data
    AlarmFetcher: Fetch alarm and threshold data
    OutputManager: Handle file I/O and atomic writes
    GaugeCollector: Facade for rain gauge collection (orchestrates others)
    RadarCollector: Facade for radar data collection (TODO)

Author: Auckland Council Internship Team (COMPSCI 778)
Date: 2026-01-21
Version: 2.0.0 - Split from monolithic collector.py (Phase 5 of SOLID refactoring)
"""

from .base import BaseCollector
from .asset_fetcher import AssetFetcher
from .trace_fetcher import TraceFetcher
from .alarm_fetcher import AlarmFetcher
from .output_manager import OutputManager
from .gauge_collector import GaugeCollector, CollectionError
from .radar_collector import RadarCollector
from .rainfall_trace_filter import (
    RainfallTraceFilter,
    FilterCriteria,
    FilterResult,
    TracesInfoProvider,
)

__all__ = [
    "BaseCollector",
    "AssetFetcher",
    "TraceFetcher",
    "AlarmFetcher",
    "OutputManager",
    "GaugeCollector",
    "RadarCollector",
    "CollectionError",
    # Pre-filter optimization (Sam's method)
    "RainfallTraceFilter",
    "FilterCriteria",
    "FilterResult",
    "TracesInfoProvider",
]

__version__ = "2.0.0"
