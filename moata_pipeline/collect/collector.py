"""
Data Collection Module (Backwards Compatibility Layer)

DEPRECATED: This module provides backwards compatibility with the old collector interface.
New code should use moata_pipeline.collect.collectors instead.

For new code, use:
    from moata_pipeline.collect.collectors import GaugeCollector

This module now delegates to the new specialized collector architecture.

Migration Guide:
---------------
OLD:
    from moata_pipeline.collect.collector import RainGaugeCollector
    collector = RainGaugeCollector(client)
    
NEW:
    from moata_pipeline.collect.collectors import GaugeCollector
    collector = GaugeCollector(client)

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 2.0.0 - Phase 5: Now uses specialized collectors (reduced from 1995 to 200 lines)
"""

from __future__ import annotations

import warnings
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import new collectors
from moata_pipeline.collect.collectors import (
    GaugeCollector as NewGaugeCollector,
    CollectionError as NewCollectionError
)
from moata_pipeline.moata.client import MoataClient

# Version info
__version__ = "2.0.0"

# Setup logger
_logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions (Backwards Compatibility)
# =============================================================================

class CollectionError(Exception):
    """
    Base exception for collection errors.
    
    DEPRECATED: Use moata_pipeline.collect.collectors.CollectionError instead.
    """
    pass


class GeometryError(CollectionError):
    """Raised when geometry processing fails."""
    pass


class CacheError(CollectionError):
    """Raised when cache operations fail."""
    pass


# =============================================================================
# Rain Gauge Collector (Backwards Compatibility Wrapper)
# =============================================================================

class RainGaugeCollector:
    """
    Collector for rain gauge data (BACKWARDS COMPATIBILITY WRAPPER).
    
    DEPRECATED: This class now delegates to the new GaugeCollector.
    
    For new code, use:
        from moata_pipeline.collect.collectors import GaugeCollector
    
    This wrapper maintains backwards compatibility while using the new
    specialized collector architecture internally:
    - AssetFetcher: Fetch gauge assets
    - TraceFetcher: Fetch traces and timeseries
    - AlarmFetcher: Fetch alarms and thresholds
    - OutputManager: Handle file I/O
    - GaugeCollector: Orchestrate the above
    
    Example (old style - still works):
        >>> from moata_pipeline.collect.collector import RainGaugeCollector
        >>> collector = RainGaugeCollector(client)
        >>> data = collector.collect(project_id=594, asset_type_id=100)
    
    Example (new style - recommended):
        >>> from moata_pipeline.collect.collectors import GaugeCollector
        >>> collector = GaugeCollector(client)
        >>> data = collector.collect(project_id=594, asset_type_id=100)
    """

    def __init__(self, client: MoataClient, output_dir: Optional[Path] = None) -> None:
        """
        Initialize rain gauge collector.

        Args:
            client: Authenticated MoataClient instance
            output_dir: Optional output directory for atomic writes
        """
        # Delegate to new GaugeCollector
        self._collector = NewGaugeCollector(client, output_dir)
        self._logger = logging.getLogger(f"{__name__}.RainGaugeCollector")
        
        # Show deprecation warning (only once per session)
        warnings.warn(
            "RainGaugeCollector from moata_pipeline.collect.collector is deprecated. "
            "Use 'from moata_pipeline.collect.collectors import GaugeCollector' instead. "
            "The old interface will be removed in v3.0.0.",
            DeprecationWarning,
            stacklevel=2
        )
        
        self._logger.debug("RainGaugeCollector initialized (delegating to new GaugeCollector)")

    def collect(
        self,
        project_id: int,
        asset_type_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        trace_batch_size: int = 100,
        fetch_thresholds: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Collect complete rain gauge data.
        
        DEPRECATED: Delegates to GaugeCollector.collect()
        
        Args:
            project_id: Moata project ID
            asset_type_id: Asset type ID for rain gauges
            start_time: Start of time range (default: 24 hours ago)
            end_time: End of time range (default: now)
            trace_batch_size: Number of assets per batch
            fetch_thresholds: Whether to fetch alarm thresholds

        Returns:
            List of gauge data dictionaries with traces, alarms, thresholds
            
        Example:
            >>> data = collector.collect(594, 25)
            >>> print(f"Collected {len(data)} gauges")
        """
        self._logger.debug("Delegating collect() to new GaugeCollector")
        return self._collector.collect(
            project_id=project_id,
            asset_type_id=asset_type_id,
            start_time=start_time,
            end_time=end_time,
            trace_batch_size=trace_batch_size,
            fetch_thresholds=fetch_thresholds
        )

    def setup_temp_dir(self) -> None:
        """Setup temporary directory (delegates to OutputManager)."""
        self._logger.debug("Delegating setup_temp_dir() to OutputManager")
        self._collector.setup_temp_dir()

    def cleanup_temp_dir(self) -> None:
        """Cleanup temporary directory (delegates to OutputManager)."""
        self._logger.debug("Delegating cleanup_temp_dir() to OutputManager")
        self._collector.cleanup_temp_dir()

    def finalize_output(self) -> None:
        """Finalize output (delegates to OutputManager)."""
        self._logger.debug("Delegating finalize_output() to OutputManager")
        self._collector.finalize_output()


# =============================================================================
# Radar Collector (Placeholder - Not Yet Refactored)
# =============================================================================

# NOTE: RadarDataCollector will be refactored in Phase 8
# For now, keep the original implementation separate

class RadarDataCollector:
    """
    Radar data collector placeholder.
    
    TODO: This will be refactored in Phase 8 to use specialized collectors
    similar to how RainGaugeCollector was refactored.
    
    For now, use the original implementation from collector_original.py
    or wait for Phase 8 refactoring.
    """
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "RadarDataCollector is not yet refactored. "
            "This will be implemented in Phase 8 using specialized collectors. "
            "For now, please use the original collector_original.py file if needed."
        )


def _ensure_aware_utc(dt: datetime) -> datetime:
    """
    Ensure datetime is timezone-aware UTC.
    
    Helper function for backwards compatibility.
    """
    from datetime import timezone
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


__all__ = [
    "RainGaugeCollector",
    "RadarDataCollector",
    "CollectionError",
    "GeometryError",
    "CacheError",
]
