"""
Data Collection Module

Provides collectors for rain gauge and radar QPE (Quantitative Precipitation
Estimation) data from Moata API.

Classes:
    RainGaugeCollector: Collects rain gauge assets, traces, alarms, and thresholds
    RadarDataCollector: Collects radar QPE data for stormwater catchments

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-02
Version: 1.1.0 - Added atomic writes with temp folder for radar data
"""

from __future__ import annotations

import json
import logging
import pickle
import time
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from moata_pipeline.moata.client import MoataClient
from moata_pipeline.common.paths import PipelinePaths, get_paths
from moata_pipeline.common.typing_utils import safe_int
from moata_pipeline.common.time_utils import iso_z
from moata_pipeline.common.text_utils import safe_filename
from moata_pipeline.common.iter_utils import chunk
from moata_pipeline.common.spatial_utils import (
    estimate_pixel_area_weights_simple,
    save_pixel_weights
)

# Optional: shapely for geometry simplification
try:
    from shapely import wkt as shapely_wkt
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False


# Version info
__version__ = "1.1.0"


# =============================================================================
# Custom Exceptions
# =============================================================================

class CollectionError(Exception):
    """Base exception for collection errors."""
    pass


class GeometryError(CollectionError):
    """Raised when geometry processing fails."""
    pass


class CacheError(CollectionError):
    """Raised when cache operations fail."""
    pass


def _ensure_aware_utc(dt: datetime) -> datetime:
    """
    Ensure datetime is timezone-aware and converted to UTC.

    - If dt is naive: assume UTC.
    - If aware: convert to UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# =============================================================================
# Rain Gauge Collector
# =============================================================================

class RainGaugeCollector:
    """
    Collector for rain gauge data with traces, alarms, and thresholds.

    Fetches:
        - Rain gauge assets (sensors)
        - Traces for each gauge (measurement series)
        - Alarms configured for each trace
        - Alarm thresholds
        - Project-level detailed alarms

    Args:
        client: Authenticated MoataClient instance

    Example:
        >>> from moata_pipeline.moata import create_client
        >>> client = create_client(client_id="...", client_secret="...")
        >>> collector = RainGaugeCollector(client)
        >>> data = collector.collect(project_id=594, asset_type_id=100)
        >>> print(f"Collected {len(data)} gauges")
    """

    def __init__(self, client: MoataClient, output_dir: Optional[Path] = None) -> None:
        """
        Initialize rain gauge collector.

        Args:
            client: Authenticated MoataClient instance
            output_dir: Optional output directory for atomic writes
        """
        if not isinstance(client, MoataClient):
            raise TypeError(
                f"client must be MoataClient instance, got {type(client).__name__}"
            )

        self._client = client
        self._logger = logging.getLogger(f"{__name__}.RainGaugeCollector")

        # Setup for atomic writes (same pattern as RadarDataCollector)
        self._base_output_dir = Path(output_dir) if output_dir else None
        self._temp_dir: Optional[Path] = None

        if self._base_output_dir:
            self._temp_dir = self._base_output_dir / "_temp_raw"

    # ============================================================================
    # ← MODIFIED: collect() method - added time range parameters
    # ============================================================================

    def collect(
        self,
        project_id: int,
        asset_type_id: int,
        start_time: Optional[datetime] = None,  # ← ADDED
        end_time: Optional[datetime] = None,    # ← ADDED
        trace_batch_size: int = 100,
        fetch_thresholds: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Collect complete rain gauge data including traces, alarms, and thresholds.

        Args:
            project_id: Moata project ID
            asset_type_id: Asset type ID for rain gauges (typically 100)
            start_time: Start of time range (default: 24 hours ago)  # ← ADDED
            end_time: End of time range (default: now)                # ← ADDED
            trace_batch_size: Number of assets to fetch traces for per batch
            fetch_thresholds: Whether to fetch alarm thresholds (slower)

        Returns:
            List of dictionaries, each containing:
                - gauge: Asset information
                - traces: List of trace data with alarms, thresholds, and timeseries

        Raises:
            ValueError: If project_id or asset_type_id are invalid
            CollectionError: If collection fails

        Example:
            >>> data = collector.collect(
            ...     project_id=594,
            ...     asset_type_id=100,
            ...     start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            ...     end_time=datetime(2025, 1, 2, tzinfo=timezone.utc),
            ...     trace_batch_size=50,
            ...     fetch_thresholds=False
            ... )
        """
        # Validate inputs
        if not isinstance(project_id, int) or project_id <= 0:
            raise ValueError(f"project_id must be positive int, got {project_id}")

        if not isinstance(asset_type_id, int) or asset_type_id <= 0:
            raise ValueError(f"asset_type_id must be positive int, got {asset_type_id}")

        if not isinstance(trace_batch_size, int) or trace_batch_size <= 0:
            raise ValueError(
                f"trace_batch_size must be positive int, got {trace_batch_size}"
            )

        # ← ADDED: Set default time range if not provided
        if end_time is None:
            end_time = datetime.now(timezone.utc)
        if start_time is None:
            start_time = end_time - timedelta(hours=24)

        # Ensure timezone-aware UTC (prevents iso_z surprises)
        start_time = _ensure_aware_utc(start_time)
        end_time = _ensure_aware_utc(end_time)

        self._logger.info("Starting rain gauge collection...")
        self._logger.info(f"  Project ID: {project_id}")
        self._logger.info(f"  Asset Type ID: {asset_type_id}")

        # ← ADDED: Log time range
        self._logger.info("  Time Range:")
        self._logger.info(f"    Start: {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self._logger.info(f"    End:   {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self._logger.info(
            f"    Duration: {(end_time - start_time).total_seconds() / 3600:.1f} hours"
        )

        self._logger.info(f"  Trace Batch Size: {trace_batch_size}")
        self._logger.info(f"  Fetch Thresholds: {fetch_thresholds}")

        # If configured, use atomic temp folder strategy for gauge output as well
        temp_enabled = self._temp_dir is not None

        if temp_enabled:
            self.setup_temp_dir()

        try:
            # 1) Fetch all rain gauge assets
            gauges = self._fetch_gauges(project_id, asset_type_id)

            # 2) Fetch project-level detailed alarms
            detailed_by_trace = self._fetch_detailed_alarms(project_id)

            # 3) Extract asset IDs and create lookup
            asset_ids, gauge_by_id = self._prepare_asset_lookup(gauges)

            if not asset_ids:
                self._logger.warning("No valid asset IDs found")
                return []

            # 4) Fetch traces in batches
            traces_by_asset = self._fetch_traces_batched(asset_ids, trace_batch_size)

            # 5) Enrich each gauge with trace data, alarms, and thresholds
            # ← MODIFIED: Pass time parameters
            all_data = self._enrich_gauges_with_traces(
                asset_ids=asset_ids,
                gauge_by_id=gauge_by_id,
                traces_by_asset=traces_by_asset,
                detailed_by_trace=detailed_by_trace,
                fetch_thresholds=fetch_thresholds,
                start_time=start_time,  # ← ADDED
                end_time=end_time,      # ← ADDED
            )

            self._logger.info(f"✓ Collection complete: {len(all_data)} gauges")

            # If atomic output is configured, write JSON to temp then finalize
            if temp_enabled and self._temp_dir is not None:
                temp_json = self._temp_dir / "rain_gauges_traces_alarms.json"
                try:
                    with open(temp_json, "w", encoding="utf-8") as f:
                        json.dump(all_data, f, indent=2, default=str)
                    self._logger.info(f"✓ Wrote gauge data to temp: {temp_json}")
                except Exception as e:
                    self._logger.error(f"Failed to write gauge JSON to temp: {e}")
                    raise

                self.finalize_output()

            return all_data

        except Exception as e:
            self._logger.error(f"Collection failed: {e}")
            # If temp enabled, keep temp data for recovery (do not auto-delete)
            raise CollectionError(f"Failed to collect rain gauge data: {e}") from e

    # ← EXISTING: Keep all other methods unchanged until _enrich_gauges_with_traces
    # _fetch_gauges() - NO CHANGES
    # _fetch_detailed_alarms() - NO CHANGES
    # _prepare_asset_lookup() - NO CHANGES
    # _fetch_traces_batched() - NO CHANGES

    def _fetch_gauges(self, project_id: int, asset_type_id: int) -> List[Dict[str, Any]]:
        """Fetch rain gauge assets."""
        self._logger.info("Fetching rain gauges...")

        gauges = self._client.get_rain_gauges(
            project_id=project_id,
            asset_type_id=asset_type_id
        )

        self._logger.info(f"✓ Fetched {len(gauges)} rain gauges")
        return gauges

    def _fetch_detailed_alarms(self, project_id: int) -> Dict[int, Dict[str, Any]]:
        """Fetch project-level detailed alarms indexed by trace ID."""
        self._logger.info("Fetching detailed alarms...")

        detailed_by_trace = self._client.get_detailed_alarms_by_project(project_id)

        self._logger.info(f"✓ Fetched {len(detailed_by_trace)} detailed alarms")
        return detailed_by_trace

    def _prepare_asset_lookup(
        self,
        gauges: List[Dict[str, Any]]
    ) -> Tuple[List[int], Dict[int, Dict[str, Any]]]:
        """
        Extract asset IDs and create lookup dictionary.

        Returns:
            Tuple of (asset_ids_list, gauge_by_id_dict)
        """
        asset_ids: List[int] = []
        gauge_by_id: Dict[int, Dict[str, Any]] = {}

        for g in gauges:
            asset_id = g.get("id") or g.get("assetId")
            asset_id_int = safe_int(asset_id)

            if asset_id_int is None:
                self._logger.warning(f"Gauge without valid ID: {g.get('name', 'Unknown')}")
                continue

            asset_ids.append(asset_id_int)
            gauge_by_id[asset_id_int] = g

        self._logger.info(f"✓ Prepared {len(asset_ids)} valid asset IDs")
        return asset_ids, gauge_by_id

    def _fetch_traces_batched(
        self,
        asset_ids: List[int],
        batch_size: int
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Fetch traces for assets in batches.

        Returns:
            Dictionary mapping asset_id -> list of traces
        """
        self._logger.info(f"Fetching traces in batches of {batch_size}...")

        all_traces: List[Dict[str, Any]] = []
        num_batches = (len(asset_ids) + batch_size - 1) // batch_size

        for batch_idx, batch in enumerate(chunk(asset_ids, batch_size), start=1):
            self._logger.debug(f"  Batch {batch_idx}/{num_batches}: {len(batch)} assets")

            try:
                traces = self._client.get_traces_for_assets(batch)
                all_traces.extend(traces)
            except Exception as e:
                self._logger.error(f"  Failed to fetch batch {batch_idx}: {e}")
                # Continue with other batches

        self._logger.info(f"✓ Fetched {len(all_traces)} traces total")

        # Group traces by asset ID
        traces_by_asset: Dict[int, List[Dict[str, Any]]] = {}
        for t in all_traces:
            asset_id = safe_int(t.get("assetId"))
            if asset_id is None:
                continue
            traces_by_asset.setdefault(asset_id, []).append(t)

        return traces_by_asset

    # ============================================================================
    # ← MODIFIED: _enrich_gauges_with_traces() - added time parameters
    # ============================================================================

    def _enrich_gauges_with_traces(
        self,
        asset_ids: List[int],
        gauge_by_id: Dict[int, Dict[str, Any]],
        traces_by_asset: Dict[int, List[Dict[str, Any]]],
        detailed_by_trace: Dict[int, Dict[str, Any]],
        fetch_thresholds: bool,
        start_time: Optional[datetime] = None,  # ← ADDED
        end_time: Optional[datetime] = None,    # ← ADDED
    ) -> List[Dict[str, Any]]:
        """Enrich each gauge with its traces, alarms, thresholds, and timeseries."""
        all_data: List[Dict[str, Any]] = []

        for idx, asset_id in enumerate(asset_ids, start=1):
            gauge = gauge_by_id.get(asset_id, {})
            name = gauge.get("name", "Unknown")

            self._logger.info(
                f"Processing [{idx}/{len(asset_ids)}]: {name} (ID: {asset_id})"
            )

            traces = traces_by_asset.get(asset_id, [])
            traces_out: List[Dict[str, Any]] = []

            for trace in traces:
                # ← MODIFIED: Pass time parameters
                enriched_trace = self._enrich_single_trace(
                    trace=trace,
                    detailed_by_trace=detailed_by_trace,
                    fetch_thresholds=fetch_thresholds,
                    start_time=start_time,  # ← ADDED
                    end_time=end_time,      # ← ADDED
                )

                if enriched_trace:
                    traces_out.append(enriched_trace)

            all_data.append({"gauge": gauge, "traces": traces_out})

        return all_data

    # ============================================================================
    # ← MODIFIED: _enrich_single_trace() - added timeseries fetching
    # ============================================================================

    def _enrich_single_trace(
        self,
        trace: Dict[str, Any],
        detailed_by_trace: Dict[int, Dict[str, Any]],
        fetch_thresholds: bool,
        start_time: Optional[datetime] = None,  # ← ADDED
        end_time: Optional[datetime] = None,    # ← ADDED
    ) -> Optional[Dict[str, Any]]:
        """Enrich a single trace with alarms, thresholds, and timeseries data."""
        trace_id = trace.get("id") or trace.get("traceId")
        trace_id_int = safe_int(trace_id)

        if trace_id_int is None:
            self._logger.warning("  Trace without valid ID, skipping")
            return None

        has_alarms = bool(trace.get("hasAlarms", False))

        alarms_raw: List[Dict[str, Any]] = []
        alarms_split: Dict[str, List[Dict[str, Any]]] = {
            "overflow": [],
            "recency": [],
            "other": []
        }
        thresholds: List[Dict[str, Any]] = []

        # ← EXISTING: Fetch alarms and thresholds (unchanged)
        if has_alarms:
            try:
                alarms_raw = self._client.get_alarms_for_trace(trace_id_int)
                alarms_split = self._client.split_alarms_by_type(alarms_raw)

                if fetch_thresholds:
                    thresholds = self._client.get_thresholds_for_trace(trace_id_int)

            except Exception as e:
                self._logger.warning(f"  Failed to fetch alarms for trace {trace_id_int}: {e}")

        detailed_alarm = detailed_by_trace.get(trace_id_int)

        # ============================================================================
        # ← ADDED: Fetch timeseries data if time range specified
        # ============================================================================
        timeseries_data = []
        if start_time and end_time:
            try:
                # Convert datetime to ISO 8601 string format
                from_time_str = iso_z(_ensure_aware_utc(start_time))
                to_time_str = iso_z(_ensure_aware_utc(end_time))

                self._logger.debug(
                    f"  Fetching timeseries for trace {trace_id_int}: {from_time_str} to {to_time_str}"
                )

                # Get trace data
                trace_data = self._client.get_trace_data(
                    trace_id=trace_id_int,
                    from_time=from_time_str,
                    to_time=to_time_str,
                    data_interval=60,
                )

                # Extract items from response
                timeseries_data = trace_data.get("items", [])

                if timeseries_data:
                    self._logger.debug(
                        f"  ✓ Fetched {len(timeseries_data)} data points for trace {trace_id_int}"
                    )
                else:
                    self._logger.debug(
                        f"  No timeseries data available for trace {trace_id_int}"
                    )

            except Exception as e:
                self._logger.warning(
                    f"  Failed to fetch timeseries for trace {trace_id_int}: {e}"
                )

        # ← MODIFIED: Return with timeseries field added
        return {
            "trace": trace,
            "alarms": alarms_raw,
            "alarms_by_type": alarms_split,
            "detailed_alarm": detailed_alarm,
            "thresholds": thresholds,
            "timeseries": timeseries_data,  # ← ADDED
        }

    def setup_temp_dir(self) -> None:
        """Setup temporary directory for atomic writes with robust cleanup."""
        import time

        if self._temp_dir is None:
            # No temp dir configured, skip
            return

        if self._temp_dir.exists():
            self._logger.warning(f"Removing existing temp directory: {self._temp_dir}")

            # Strategy: Try progressively more aggressive approaches
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Try normal removal
                    shutil.rmtree(self._temp_dir)
                    self._logger.debug("✓ Successfully removed temp directory")
                    break

                except PermissionError as e:
                    self._logger.warning(
                        f"Attempt {attempt + 1}/{max_retries}: Permission denied - {e}"
                    )

                    if attempt < max_retries - 1:
                        # Wait and retry (files might be temporarily locked)
                        time.sleep(2)
                        continue

                    # Last attempt: Try to work around the lock
                    self._logger.warning("Trying alternative cleanup strategies...")

                    # Strategy 1: Rename locked directory and create new one
                    try:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        old_dir = self._temp_dir.parent / f"_temp_raw_old_{timestamp}"

                        # Try to rename the locked directory
                        self._temp_dir.rename(old_dir)
                        self._logger.warning(
                            f"Could not delete temp directory. Renamed to: {old_dir}"
                        )
                        self._logger.warning(
                            "Please manually delete this folder later when files are released."
                        )
                        break

                    except Exception as rename_error:
                        self._logger.error(f"Rename strategy failed: {rename_error}")

                        # Last resort: Inform user and continue
                        self._logger.error(
                            f"Unable to clean temp directory: {self._temp_dir}\n"
                            f"Please close any programs accessing this folder and try again.\n"
                            f"Or manually delete it before running collection."
                        )
                        raise PermissionError(
                            f"Cannot access temp directory: {self._temp_dir}\n"
                            f"Close File Explorer or other programs using this folder."
                        ) from e

        # Create fresh temp directory
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        self._logger.debug(f"✓ Temp directory ready: {self._temp_dir}")

    def cleanup_temp_dir(self) -> None:
        """Remove temporary directory (on failure or cancellation)."""
        if self._temp_dir and self._temp_dir.exists():
            try:
                shutil.rmtree(self._temp_dir)
                self._logger.info(f"✓ Cleaned up temp directory: {self._temp_dir}")
            except Exception as e:
                self._logger.warning(f"Failed to clean up temp directory: {e}")

    def finalize_output(self) -> None:
        """
        Move data from temp directory to final location.

        This is called only after successful completion of collection.
        Existing data in final location is replaced atomically.
        """
        import time

        if not self._temp_dir or not self._temp_dir.exists():
            self._logger.warning("No temp directory to finalize")
            return

        self._logger.info("Finalizing output (moving from temp to final location)...")

        # For gauge, we have a single JSON file, so just move the file
        temp_json = self._temp_dir / "rain_gauges_traces_alarms.json"

        if not temp_json.exists():
            self._logger.error(f"Expected file not found in temp: {temp_json}")
            return

        # Ensure base directory exists
        if self._base_output_dir is None:
            self._logger.warning("No base output dir configured, cannot finalize")
            return

        self._base_output_dir.mkdir(parents=True, exist_ok=True)

        # Move file to final location (with retry mechanism)
        final_json = self._base_output_dir / "rain_gauges_traces_alarms.json"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                if final_json.exists():
                    self._logger.info(f"  Replacing existing file: {final_json}")
                    final_json.unlink()

                shutil.move(str(temp_json), str(final_json))
                self._logger.info(f"✓ Moved data to final location: {final_json}")
                break  # Success!

            except PermissionError as e:
                self._logger.warning(
                    f"Attempt {attempt + 1}/{max_retries}: Permission denied - {e}"
                )

                if attempt < max_retries - 1:
                    self._logger.debug("Waiting 2 seconds before retry...")
                    time.sleep(2)
                    continue

                # Last attempt failed
                self._logger.error(
                    f"\n{'='*80}\n"
                    f"⚠️  COLLECTION SUCCEEDED BUT FINALIZATION BLOCKED\n"
                    f"{'='*80}\n"
                    f"Your data is safe in: {temp_json}\n"
                    f"\n"
                    f"The existing file is locked: {final_json}\n"
                    f"\n"
                    f"To complete the process:\n"
                    f"1. Close the JSON file if it's opened in an editor\n"
                    f"2. Close File Explorer windows showing this folder\n"
                    f"3. Then manually:\n"
                    f"   - Delete: {final_json}\n"
                    f"   - Copy: {temp_json} → {final_json}\n"
                    f"{'='*80}\n"
                )
                raise PermissionError(
                    f"Cannot finalize output - file is locked: {final_json}\n"
                    f"Close any programs using this file.\n"
                    f"Your data is safe in: {temp_json}"
                ) from e

        # Clean up temp directory
        try:
            shutil.rmtree(self._temp_dir)
            self._logger.debug(f"✓ Removed temp directory: {self._temp_dir}")
        except Exception as e:
            self._logger.warning(f"Failed to remove temp directory: {e}")

        self._logger.info("✓ Finalization complete")


# =============================================================================
# Radar Data Collector
# =============================================================================

class RadarDataCollector:
    """
    Collector for radar QPE (Quantitative Precipitation Estimation) data.

    Collects spatial rainfall data from radar for stormwater catchments,
    including pixel mapping and timeseries data.

    IMPORTANT: This collector uses atomic writes - data is written to a
    temporary folder first, then moved to the final location only after
    successful completion. This ensures existing data is not corrupted
    if the collection is cancelled or fails.

    Output Structure:
        outputs/rain_radar/raw/
        ├── catchments/
        │   └── stormwater_catchments.csv
        ├── pixel_mappings/
        │   ├── catchment_pixel_mapping.json
        │   └── catchment_pixel_mapping.pkl
        ├── radar_data/
        │   └── {catchment_id}_{catchment_name}.csv
        └── collection_summary.json

    Configuration Notes (from Sam):
        - Use collection_id=1, traceset_id=3 for QPE data
        - Pixel indices don't change, can be cached
        - Max 150 pixels per request (recommend 50)
        - Max 24 hours of data per request
        - Data is minute resolution per pixel

    Args:
        client: Authenticated MoataClient instance
        output_dir: Base output directory (default: outputs/rain_radar/raw)
        pixel_batch_size: Pixels per API request (default: 50)
        max_hours_per_request: Maximum hours per request (default: 24)

    Example:
        >>> collector = RadarDataCollector(client, output_dir=Path("custom/output"))
        >>> results = collector.collect_all(
        ...     project_id=594,
        ...     start_time=datetime(2025, 5, 9, tzinfo=timezone.utc),
        ...     end_time=datetime(2025, 5, 10, tzinfo=timezone.utc)
        ... )
    """

    # Sam's recommended IDs
    DEFAULT_COLLECTION_ID = 1
    DEFAULT_TRACESET_ID = 3
    DEFAULT_CATCHMENT_ASSET_TYPE_ID = 3541

    # URL length limit (conservative)
    MAX_WKT_LENGTH = 14000

    def __init__(
        self,
        client: MoataClient,
        output_dir: Optional[Path] = None,
        pixel_batch_size: int = 50,
        max_hours_per_request: int = 24,
    ) -> None:
        """ Initialize radar data collector.

        Args:
            client: Authenticated MoataClient instance
            output_dir: Base output directory
            pixel_batch_size: Number of pixels per API request (1-150, recommend 50)
            max_hours_per_request: Maximum hours per request (1-24)

        Raises:
            TypeError: If client is not MoataClient
            ValueError: If batch_size or max_hours are out of range"""
        if not isinstance(client, MoataClient):
            raise TypeError(
                f"client must be MoataClient instance, got {type(client).__name__}"
            )

        if not 1 <= pixel_batch_size <= 150:
            raise ValueError(
                f"pixel_batch_size must be 1-150, got {pixel_batch_size}"
            )

        if not 1 <= max_hours_per_request <= 24:
            raise ValueError(
                f"max_hours_per_request must be 1-24, got {max_hours_per_request}"
            )

        self._client = client
        # Use get_paths() for smart default
        if output_dir is None:
            paths = get_paths()
            output_dir = paths.rain_radar_raw_dir
        self._base_output_dir = Path(output_dir)  
        self._pixel_batch_size = pixel_batch_size
        self._max_hours_per_request = max_hours_per_request
        self._logger = logging.getLogger(f"{__name__}.RadarDataCollector")

        # Temporary directory for atomic writes
        # Temp directory must NOT be inside final output dir, otherwise it gets deleted during cleanup.
        self._temp_dir: Optional[Path] = self._base_output_dir.parent / "_temp_raw"

        # Working directories (will point to temp during collection)
        self._catchments_dir: Optional[Path] = None
        self._pixel_mappings_dir: Optional[Path] = None
        self._radar_data_dir: Optional[Path] = None

        # Cache for pixel mappings (catchment_id -> [pixel_indices])
        self._pixel_cache: Dict[int, List[int]] = {}

        # Catchments cache
        self._catchments: List[Dict[str, Any]] = []

        self._logger.info("RadarDataCollector initialized")
        self._logger.info(f"  Output directory: {self._base_output_dir}")
        self._logger.info(f"  Pixel batch size: {self._pixel_batch_size}")

    def setup_temp_dir(self) -> None:
        """Setup temporary directory for atomic writes with robust cleanup."""
        import time
        import uuid  # intentionally kept (existing)

        if self._temp_dir.exists():
            self._logger.warning(f"Removing existing temp directory: {self._temp_dir}")

            # Strategy: Try progressively more aggressive approaches
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Try normal removal
                    shutil.rmtree(self._temp_dir)
                    self._logger.debug("✓ Successfully removed temp directory")
                    break

                except PermissionError as e:
                    self._logger.warning(
                        f"Attempt {attempt + 1}/{max_retries}: Permission denied - {e}"
                    )

                    if attempt < max_retries - 1:
                        # Wait and retry (files might be temporarily locked)
                        time.sleep(2)
                        continue

                    # Last attempt: Try to work around the lock
                    self._logger.warning("Trying alternative cleanup strategies...")

                    # Strategy 1: Rename locked directory and create new one
                    try:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        old_dir = self._temp_dir.parent / f"_temp_raw_old_{timestamp}"

                        # Try to rename the locked directory
                        self._temp_dir.rename(old_dir)
                        self._logger.warning(
                            f"Could not delete temp directory. Renamed to: {old_dir}"
                        )
                        self._logger.warning(
                            "Please manually delete this folder later when files are released."
                        )
                        break

                    except Exception as rename_error:
                        self._logger.error(f"Rename strategy failed: {rename_error}")

                        # Strategy 2: Try to clear contents individually
                        try:
                            self._logger.warning("Attempting to clear locked files individually...")
                            cleared = 0
                            failed = []

                            for item in self._temp_dir.rglob('*'):
                                if item.is_file():
                                    try:
                                        item.unlink()
                                        cleared += 1
                                    except Exception:
                                        failed.append(str(item))

                            if cleared > 0:
                                self._logger.info(f"Cleared {cleared} files, {len(failed)} remain locked")

                            if failed:
                                self._logger.warning(
                                    f"Could not remove {len(failed)} locked files. "
                                    f"Collection will proceed but cleanup incomplete."
                                )

                        except Exception as clear_error:
                            self._logger.error(f"Individual file cleanup failed: {clear_error}")

                            # Last resort: Inform user and continue
                            self._logger.error(
                                f"Unable to clean temp directory: {self._temp_dir}\n"
                                f"Please close any programs accessing this folder and try again.\n"
                                f"Or manually delete it before running collection."
                            )
                            raise PermissionError(
                                f"Cannot access temp directory: {self._temp_dir}\n"
                                f"Close File Explorer or other programs using this folder."
                            ) from e

        # Create fresh temp directory
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        # Point working dirs to TEMP so all writes go to temp during collection
        self._catchments_dir = self._temp_dir / "catchments"
        self._pixel_mappings_dir = self._temp_dir / "pixel_mappings"
        self._radar_data_dir = self._temp_dir / "radar_data"

        # Ensure temp subfolders exist
        self._ensure_dirs()

        self._logger.debug(f"✓ Temp directory ready: {self._temp_dir}")

    def cleanup_temp_dir(self) -> None:
        """Remove temporary directory (on failure or cancellation)."""
        if self._temp_dir and self._temp_dir.exists():
            try:
                shutil.rmtree(self._temp_dir)
                self._logger.info(f"✓ Cleaned up temp directory: {self._temp_dir}")
            except Exception as e:
                self._logger.warning(f"Failed to clean up temp directory: {e}")

    def finalize_output(self) -> None:
        """
        Move data from temp directory to final location.

        This is called only after successful completion of collection.
        Existing data in final location is replaced atomically.
        """
        import time

        if not self._temp_dir or not self._temp_dir.exists():
            self._logger.warning("No temp directory to finalize")
            return

        # ========================================================================
        # ✅ FIX: Convert to absolute paths to avoid working directory issues
        # ========================================================================
        self._temp_dir = self._temp_dir.resolve()
        self._base_output_dir = self._base_output_dir.resolve()

        self._logger.info("Finalizing output (moving from temp to final location)...")
        self._logger.debug(f"  Temp dir (absolute): {self._temp_dir}")
        self._logger.debug(f"  Base dir (absolute): {self._base_output_dir}")

        # Verify temp dir exists with absolute path
        if not self._temp_dir.exists():
            self._logger.error(f"❌ Temp directory not found (absolute path): {self._temp_dir}")

            # Try to find it with relative path (aligned with new temp location)
            relative_temp = self._base_output_dir.parent / "_temp_raw"
            if relative_temp.exists():
                self._logger.warning(f"Found with relative path: {relative_temp}")
                self._temp_dir = relative_temp.resolve()
            else:
                raise FileNotFoundError(
                    f"Temp directory not found:\n"
                    f"  Absolute: {self._temp_dir}\n"
                    f"  Relative: {relative_temp}\n"
                    f"Working directory: {Path.cwd()}"
                )

        # ========================================================================
        # STEP 1: Handle existing output directory
        # ========================================================================
        if self._base_output_dir.exists():
            max_retries = 3
            successfully_removed = False

            for attempt in range(max_retries):
                try:
                    self._logger.info(f"  Removing existing output: {self._base_output_dir}")
                    shutil.rmtree(self._base_output_dir)
                    self._logger.debug("✓ Successfully removed existing directory")
                    successfully_removed = True
                    break  # Success!

                except PermissionError as e:
                    self._logger.warning(
                        f"Attempt {attempt + 1}/{max_retries}: Permission denied - {e}"
                    )

                    if attempt < max_retries - 1:
                        # Wait and retry (files might be temporarily locked)
                        self._logger.debug("Waiting 2 seconds before retry...")
                        time.sleep(2)
                        continue

                    # Last attempt failed - rename the locked folder
                    self._logger.warning("Trying alternative cleanup strategies...")
                    try:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        old_dir = self._base_output_dir.parent / f"raw_old_{timestamp}"

                        # Rename the locked directory
                        self._base_output_dir.rename(old_dir)

                        self._logger.warning(
                            f"Could not delete existing folder. Renamed to: {old_dir}"
                        )
                        self._logger.warning(
                            "Please manually delete this folder later when files are released.\n"
                            "Close File Explorer or any programs accessing the old folder."
                        )
                        successfully_removed = True  # Treated as success (renamed)
                        break

                    except Exception as rename_error:
                        self._logger.error(f"Rename strategy failed: {rename_error}")

                        # Last resort: Keep temp data and inform user
                        self._logger.error(
                            f"\n{'='*80}\n"
                            f"⚠️  COLLECTION SUCCEEDED BUT FINALIZATION BLOCKED\n"
                            f"{'='*80}\n"
                            f"Your data is safe in: {self._temp_dir}\n"
                            f"\n"
                            f"The existing folder is locked: {self._base_output_dir}\n"
                            f"\n"
                            f"To complete the process:\n"
                            f"1. Close File Explorer windows showing this folder\n"
                            f"2. Close any CSV files opened from this folder\n"
                            f"3. Close any other programs accessing these files\n"
                            f"4. Then manually:\n"
                            f"   - Delete or rename: {self._base_output_dir}\n"
                            f"   - Rename: {self._temp_dir} → {self._base_output_dir}\n"
                            f"{'='*80}\n"
                        )
                        raise PermissionError(
                            f"Cannot finalize output - folder is locked: {self._base_output_dir}\n"
                            f"Close File Explorer or other programs using this folder.\n"
                            f"Your data is safe in: {self._temp_dir}"
                        ) from e

            if not successfully_removed:
                raise RuntimeError(f"Failed to remove or rename existing directory: {self._base_output_dir}")

        # ========================================================================
        # STEP 2: Ensure parent directory exists
        # ========================================================================
        parent_dir = self._base_output_dir.parent

        try:
            if not parent_dir.exists():
                self._logger.info(f"Creating parent directory: {parent_dir}")
                parent_dir.mkdir(parents=True, exist_ok=True)
                self._logger.debug(f"✓ Parent directory created: {parent_dir}")
            else:
                self._logger.debug(f"✓ Parent directory exists: {parent_dir}")
        except Exception as e:
            self._logger.error(f"Failed to ensure parent directory exists: {e}")
            self._logger.error(
                f"\nYour data is safe in: {self._temp_dir}\n"
                f"You can manually:\n"
                f"  1. Create directory: {parent_dir}\n"
                f"  2. Move: {self._temp_dir} → {self._base_output_dir}"
            )
            raise

        # ========================================================================
        # STEP 3: Double-check destination doesn't exist
        # ========================================================================
        if self._base_output_dir.exists():
            # This shouldn't happen - we just removed/renamed it!
            self._logger.warning(
                f"⚠️  Destination still exists after cleanup: {self._base_output_dir}\n"
                f"Attempting to remove it one more time..."
            )
            try:
                shutil.rmtree(self._base_output_dir)
                self._logger.info("✓ Removed unexpected destination folder")
            except Exception as cleanup_error:
                self._logger.error(f"Cannot remove destination: {cleanup_error}")
                self._logger.error(
                    f"\nYour data is safe in: {self._temp_dir}\n"
                    f"Manual fix needed:\n"
                    f"  1. Delete or rename: {self._base_output_dir}\n"
                    f"  2. Then move: {self._temp_dir} → {self._base_output_dir}"
                )
                raise PermissionError(
                    f"Destination exists and cannot be removed: {self._base_output_dir}\n"
                    f"Your data is safe in: {self._temp_dir}"
                ) from cleanup_error

        # ========================================================================
        # STEP 4: Move temp to final location (OneDrive-safe with retries)
        # ========================================================================
        max_move_retries = 5
        move_success = False

        for move_attempt in range(max_move_retries):
            try:
                # Check if source still exists (OneDrive might move it)
                if not self._temp_dir.exists():
                    if move_attempt == 0:
                        # First attempt - this is unexpected
                        self._logger.error(
                            f"⚠️  Source directory disappeared: {self._temp_dir}\n"
                            f"This often happens with OneDrive sync conflicts.\n"
                            f"Waiting and retrying..."
                        )

                    self._logger.info(f"Retry {move_attempt + 1}/{max_move_retries}: Waiting for source to reappear...")
                    time.sleep(3)  # Wait for OneDrive to finish

                    if not self._temp_dir.exists():
                        if move_attempt < max_move_retries - 1:
                            continue  # Try again
                        else:
                            # Last attempt - give up
                            self._logger.error(
                                f"\n{'='*80}\n"
                                f"❌ SOURCE DIRECTORY VANISHED\n"
                                f"{'='*80}\n"
                                f"Expected location: {self._temp_dir}\n"
                                f"\n"
                                f"This is likely caused by:\n"
                                f"1. OneDrive sync moving/renaming the folder\n"
                                f"2. Antivirus scanning and locking files\n"
                                f"3. Another process accessing the directory\n"
                                f"4. cleanup_temp_dir() was called (code bug)\n"
                                f"\n"
                                f"Solutions:\n"
                                f"1. RECOMMENDED: Move project outside OneDrive folder\n"
                                f"   Example: C:\\Projects\\moata_pipeline\n"
                                f"2. Pause OneDrive sync before running collection\n"
                                f"3. Exclude outputs folder from OneDrive sync\n"
                                f"4. Check Windows Defender / Antivirus quarantine\n"
                                f"\n"
                                f"Check if data exists in:\n"
                                f"  - {self._temp_dir}\n"
                                f"  - {self._base_output_dir.parent}\\_temp_raw (OneDrive moved it?)\n"
                                f"  - OneDrive Recycle Bin\n"
                                f"  - Windows Recycle Bin\n"
                                f"{'='*80}\n"
                            )
                            raise FileNotFoundError(
                                f"Source directory vanished during finalization: {self._temp_dir}\n"
                                f"This is likely caused by OneDrive sync or antivirus.\n"
                                f"SOLUTION: Move project outside OneDrive folder!"
                            )

                # Source exists - proceed with move
                self._logger.info(f"Move attempt {move_attempt + 1}/{max_move_retries}: {self._temp_dir} → {self._base_output_dir}")

                # Verify destination still doesn't exist (OneDrive might recreate it)
                if self._base_output_dir.exists():
                    self._logger.warning(
                        f"Destination reappeared: {self._base_output_dir}\n"
                        f"Removing it again..."
                    )
                    try:
                        shutil.rmtree(self._base_output_dir)
                    except Exception as e:
                        self._logger.error(f"Cannot remove destination: {e}")
                        if move_attempt < max_move_retries - 1:
                            time.sleep(2)
                            continue
                        raise

                # Perform the move
                shutil.move(str(self._temp_dir), str(self._base_output_dir))

                # Verify move succeeded
                if not self._base_output_dir.exists():
                    self._logger.error("Move completed but destination doesn't exist!")
                    if move_attempt < max_move_retries - 1:
                        time.sleep(2)
                        continue
                    raise RuntimeError(f"Move succeeded but destination not found: {self._base_output_dir}")

                # Success!
                self._logger.info(f"✓ Successfully moved data to: {self._base_output_dir}")
                move_success = True
                break

            except FileNotFoundError as e:
                if move_attempt < max_move_retries - 1:
                    self._logger.warning(f"Move attempt {move_attempt + 1} failed: {e}")
                    self._logger.info("Waiting 3 seconds before retry...")
                    time.sleep(3)
                    continue
                else:
                    # Already logged detailed error above
                    raise

            except Exception as e:
                self._logger.error(f"Move attempt {move_attempt + 1} failed: {e}")
                if move_attempt < max_move_retries - 1:
                    self._logger.info("Waiting 3 seconds before retry...")
                    time.sleep(3)
                    continue
                else:
                    self._logger.error(
                        f"\nAll move attempts failed.\n"
                        f"Your data should still exist in: {self._temp_dir}\n"
                        f"Check if it's there and manually rename to: {self._base_output_dir}"
                    )
                    raise

        if not move_success:
            raise RuntimeError("Failed to move temp directory after all retries")

        # ========================================================================
        # STEP 5: Update directory references
        # ========================================================================
        self._catchments_dir = self._base_output_dir / "catchments"
        self._pixel_mappings_dir = self._base_output_dir / "pixel_mappings"
        self._radar_data_dir = self._base_output_dir / "radar_data"

        self._logger.info("✓ Finalization complete")
        self._logger.debug(f"  Catchments dir: {self._catchments_dir}")
        self._logger.debug(f"  Pixel_mappings dir: {self._pixel_mappings_dir}")
        self._logger.debug(f"  Radar data dir: {self._radar_data_dir}")

    def _ensure_dirs(self) -> None:
        """Create output directories if they don't exist."""
        if self._catchments_dir is None:
            # If not using temp dir, set up final directories directly
            self._catchments_dir = self._base_output_dir / "catchments"
            self._pixel_mappings_dir = self._base_output_dir / "pixel_mappings"
            self._radar_data_dir = self._base_output_dir / "radar_data"

        for dir_path in [self._catchments_dir, self._pixel_mappings_dir, self._radar_data_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            self._logger.debug(f"  Ensured directory: {dir_path}")

    # -------------------------------------------------------------------------
    # Geometry Simplification
    # -------------------------------------------------------------------------

    def _simplify_wkt(
        self,
        wkt: str,
        max_length: int = MAX_WKT_LENGTH,
        tolerance: float = 0.0001,
    ) -> str:
        """
        Simplify WKT geometry to fit within URL length limits.

        Uses progressive simplification with increasing tolerance until
        geometry fits. Falls back to convex hull if needed.

        Args:
            wkt: WKT geometry string
            max_length: Maximum allowed string length
            tolerance: Initial simplification tolerance

        Returns:
            Simplified WKT string

        Raises:
            GeometryError: If shapely is not available or simplification fails
        """
        if not SHAPELY_AVAILABLE:
            raise GeometryError(
                "Shapely is required for geometry simplification but is not installed.\n"
                "Install with: pip install shapely"
            )

        if len(wkt) <= max_length:
            return wkt

        try:
            geom = shapely_wkt.loads(wkt)

            current_tolerance = tolerance
            max_iterations = 15

            for i in range(max_iterations):
                simplified = geom.simplify(current_tolerance, preserve_topology=True)
                simplified_wkt = simplified.wkt

                if len(simplified_wkt) <= max_length:
                    self._logger.info(
                        "    Simplified geometry: %d -> %d chars (tolerance=%.6f, iter=%d)",
                        len(wkt), len(simplified_wkt), current_tolerance, i + 1
                    )
                    return simplified_wkt

                current_tolerance *= 2

            # Last resort: use convex hull
            hull = geom.convex_hull
            hull_wkt = hull.wkt

            if len(hull_wkt) > max_length:
                raise GeometryError(
                    f"Cannot simplify geometry enough: {len(hull_wkt)} chars (max: {max_length})"
                )

            self._logger.warning(
                "    Using convex hull: %d -> %d chars",
                len(wkt), len(hull_wkt)
            )
            return hull_wkt

        except Exception as e:
            self._logger.error(f"Failed to simplify geometry: {e}")
            raise GeometryError(f"Geometry simplification failed: {e}") from e

    # -------------------------------------------------------------------------
    # Pixel Cache Management
    # -------------------------------------------------------------------------

    @property
    def _pixel_cache_json(self) -> Path:
        """Path to JSON pixel cache file."""
        if self._pixel_mappings_dir is None:
            return self._base_output_dir / "pixel_mappings" / "catchment_pixel_mapping.json"
        return self._pixel_mappings_dir / "catchment_pixel_mapping.json"

    @property
    def _pixel_cache_pkl(self) -> Path:
        """Path to pickle pixel cache file."""
        if self._pixel_mappings_dir is None:
            return self._base_output_dir / "pixel_mappings" / "catchment_pixel_mapping.pkl"
        return self._pixel_mappings_dir / "catchment_pixel_mapping.pkl"

    def _load_pixel_cache(self) -> bool:
        """
        Load cached pixel mappings from disk (from FINAL location, not temp).

        Tries pickle first (faster), then JSON (portable).

        Returns:
            True if cache was loaded successfully, False otherwise
        """
        # Always load from final location (not temp)
        pkl_path = self._base_output_dir / "pixel_mappings" / "catchment_pixel_mapping.pkl"
        json_path = self._base_output_dir / "pixel_mappings" / "catchment_pixel_mapping.json"

        # Try pickle first (faster)
        if pkl_path.exists():
            try:
                with open(pkl_path, "rb") as f:
                    self._pixel_cache = pickle.load(f)
                self._logger.info(
                    "✓ Loaded pixel cache from pkl: %d catchments",
                    len(self._pixel_cache)
                )
                return True
            except Exception as e:
                self._logger.warning(f"Failed to load pkl cache: {e}")

        # Fallback to JSON
        if json_path.exists():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._pixel_cache = {int(k): v for k, v in data.items()}
                self._logger.info(
                    "✓ Loaded pixel cache from json: %d catchments",
                    len(self._pixel_cache)
                )
                return True
            except Exception as e:
                self._logger.warning(f"Failed to load json cache: {e}")

        self._logger.info("No pixel cache found, will build from scratch")
        return False

    def _save_pixel_cache(self) -> None:
        """
        Save pixel mappings to both JSON (portable) and pickle (fast).

        Raises:
            CacheError: If both save operations fail
        """
        self._ensure_dirs()

        json_success = False
        pkl_success = False

        # Save JSON
        try:
            with open(self._pixel_cache_json, "w", encoding="utf-8") as f:
                json.dump(self._pixel_cache, f, indent=2)
            self._logger.info(
                "✓ Saved pixel cache to json: %d catchments",
                len(self._pixel_cache)
            )
            json_success = True
        except Exception as e:
            self._logger.warning(f"Failed to save json cache: {e}")

        # Save pickle
        try:
            with open(self._pixel_cache_pkl, "wb") as f:
                pickle.dump(self._pixel_cache, f)
            self._logger.info("✓ Saved pixel cache to pkl")
            pkl_success = True
        except Exception as e:
            self._logger.warning(f"Failed to save pkl cache: {e}")

        if not (json_success or pkl_success):
            raise CacheError("Failed to save pixel cache in any format")

    # -------------------------------------------------------------------------
    # Catchments
    # -------------------------------------------------------------------------

    def get_stormwater_catchments(
        self,
        project_id: int,
        asset_type_id: int = DEFAULT_CATCHMENT_ASSET_TYPE_ID,
        sr_id: int = 4326,
    ) -> List[Dict[str, Any]]:
        """
        Get stormwater catchment assets with geometry.

        Args:
            project_id: Moata project ID
            asset_type_id: Asset type for catchments (default: 3541)
            sr_id: Spatial reference ID (default: 4326 for WGS84)

        Returns:
            List of catchment dictionaries with geometry

        Raises:
            ValueError: If parameters are invalid
        """
        if not isinstance(project_id, int) or project_id <= 0:
            raise ValueError(f"project_id must be positive int, got {project_id}")

        self._logger.info(
            "Fetching stormwater catchments (project=%d, assetType=%d, srId=%d)...",
            project_id, asset_type_id, sr_id
        )

        catchments = self._client.get_assets_with_geometry(
            project_id=project_id,
            asset_type_id=asset_type_id,
            sr_id=sr_id,
        )

        self._catchments = catchments
        self._logger.info(f"✓ Fetched {len(catchments)} stormwater catchments")
        return catchments

    def save_catchments_csv(self) -> Path:
        """
        Save catchments metadata to CSV.

        Returns:
            Path to saved CSV file

        Raises:
            ValueError: If no catchments available
        """
        self._ensure_dirs()

        if not self._catchments:
            raise ValueError("No catchments to save. Call get_stormwater_catchments() first.")

        rows = []
        for c in self._catchments:
            rows.append({
                "id": c.get("id"),
                "name": c.get("name"),
                "description": c.get("description"),
                "projectId": c.get("projectId"),
                "assetType": c.get("assetType"),
                "assetTypes": str(c.get("assetTypes")) if c.get("assetTypes") else None,
                "lastModified": c.get("lastModified"),
                "modifiedBy": c.get("modifiedBy"),
                "geometrySrId": c.get("geometrySrId"),
                "geometryWkt": c.get("geometryWkt"),
            })

        df = pd.DataFrame(rows)
        out_path = self._catchments_dir / "stormwater_catchments.csv"
        df.to_csv(out_path, index=False)

        self._logger.info(f"✓ Saved {len(rows)} catchments to {out_path}")
        return out_path

    # -------------------------------------------------------------------------
    # Pixel Mappings
    # -------------------------------------------------------------------------

    def get_pixel_indices_for_catchment(
        self,
        catchment: Dict[str, Any],
        collection_id: int = DEFAULT_COLLECTION_ID,
        use_cache: bool = True,
    ) -> List[int]:
        """
        Get radar pixel indices that intersect a catchment geometry.

        Args:
            catchment: Catchment dictionary with id and geometryWkt
            collection_id: Radar collection ID (default: 1)
            use_cache: Use cached mappings if available

        Returns:
            List of pixel indices

        Raises:
            ValueError: If catchment has no valid ID or geometry
            GeometryError: If geometry simplification fails
        """
        catchment_id = safe_int(catchment.get("id"))
        if catchment_id is None:
            raise ValueError("Catchment must have valid 'id' field")

        # Check cache first
        if use_cache and catchment_id in self._pixel_cache:
            self._logger.debug(f"  Using cached pixels for catchment {catchment_id}")
            return self._pixel_cache[catchment_id]

        # Get geometry
        wkt = catchment.get("geometryWkt", "")
        if not wkt:
            raise ValueError(f"Catchment {catchment_id} has no geometry")

        # Simplify if too large
        if len(wkt) > self.MAX_WKT_LENGTH:
            self._logger.info(
                "  Geometry too large (%d chars), simplifying...",
                len(wkt)
            )
            wkt = self._simplify_wkt(wkt, max_length=self.MAX_WKT_LENGTH)

        # Fetch pixel mappings from API
        mappings = self._client.get_pixel_mappings_for_geometry(
            collection_id=collection_id,
            wkt=wkt,
            sr_id=4326,
        )

        pixel_indices = [
            m.get("pixelIndex")
            for m in mappings
            if m.get("pixelIndex") is not None
        ]

        # Cache the result
        self._pixel_cache[catchment_id] = pixel_indices

        self._logger.debug(f"  Found {len(pixel_indices)} pixels for catchment {catchment_id}")
        return pixel_indices

    # -------------------------------------------------------------------------
    # Radar Data
    # -------------------------------------------------------------------------

    def fetch_radar_data(
        self,
        pixel_indices: List[int],
        start_time: datetime,
        end_time: datetime,
        collection_id: int = DEFAULT_COLLECTION_ID,
        traceset_id: int = DEFAULT_TRACESET_ID,
    ) -> List[Dict[str, Any]]:
        """
        Fetch radar data for given pixels and time range.

        Args:
            pixel_indices: List of pixel indices
            start_time: Start time (UTC)
            end_time: End time (UTC)
            collection_id: Radar collection ID
            traceset_id: Traceset ID for QPE data

        Returns:
            List of data dictionaries with pixel values

        Raises:
            ValueError: If inputs are invalid
        """
        if not pixel_indices:
            self._logger.warning("  No pixel indices provided")
            return []

        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError("start_time and end_time must be datetime objects")

        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")

        start_str = iso_z(_ensure_aware_utc(start_time))
        end_str = iso_z(_ensure_aware_utc(end_time))

        self._logger.info(
            "  Fetching radar data: %d pixels, %s to %s",
            len(pixel_indices), start_str, end_str
        )

        data = self._client.get_traceset_data_batched(
            collection_id=collection_id,
            traceset_ids=[traceset_id],
            pixel_indices=pixel_indices,
            start_time=start_str,
            end_time=end_str,
            batch_size=self._pixel_batch_size,
        )

        self._logger.info(
            "  ✓ Fetched data for %d pixel-traceset combinations",
            len(data)
        )
        return data

    def save_catchment_radar_data(
        self,
        catchment: Dict[str, Any],
        data: List[Dict[str, Any]],
        pixel_weights: Optional[Dict[Tuple[int, int], float]] = None,  # ← ADD PARAMETER
    ) -> Optional[Path]:
        """
        Save radar data for a single catchment to CSV with optional area weighting.

        Args:
            catchment: Catchment dictionary
            data: Radar data from API
            pixel_weights: Dict mapping (catchment_id, pixel_index) -> weight
                          If None, no weighting applied (original behavior)

        Returns:
            Path to saved CSV, or None if no data
        """
        self._ensure_dirs()

        if not data:
            self._logger.debug("  No data to save")
            return None

        catchment_id = safe_int(catchment.get("id"))
        name = safe_filename(catchment.get("name", "unknown"))
        filename = f"{catchment_id}_{name}.csv"

        rows = []
        for d in data:
            pixel_index = d.get("pixelIndex")
            start_time = d.get("startTime")
            offset_seconds = d.get("dataOffsetSeconds", 60)
            values = d.get("values", [])

            if not values:
                continue

            # ========== NEW: Get weight for this pixel ==========
            weight = 1.0
            if pixel_weights is not None:
                weight = pixel_weights.get((catchment_id, pixel_index), 1.0)
            # ====================================================

            # Parse start time
            try:
                if start_time:
                    if start_time.endswith("Z"):
                        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    else:
                        start_dt = datetime.fromisoformat(start_time)
                else:
                    start_dt = None
            except Exception as e:
                self._logger.warning(f"  Failed to parse start_time: {e}")
                start_dt = None

            # Create row for each value
            for i, value in enumerate(values):
                if value is None:
                    continue

                timestamp = None
                if start_dt and offset_seconds:
                    timestamp = start_dt + timedelta(seconds=i * offset_seconds)

                # ========== MODIFIED: Apply weight and keep original ==========
                original_value = value
                weighted_value = value * weight
                
                rows.append({
                    "pixel_index": pixel_index,
                    "value_index": i,
                    "timestamp": timestamp.isoformat() if timestamp else None,
                    "value": weighted_value,        # ← Weighted value
                    "weight": weight,               # ← NEW: Include weight for transparency
                    "original_value": original_value  # ← NEW: Keep original for reference
                })
                # ==============================================================

        if not rows:
            self._logger.debug("  No valid rows after processing")
            return None

        df = pd.DataFrame(rows)
        out_path = self._radar_data_dir / filename
        df.to_csv(out_path, index=False)

        # ========== MODIFIED: Log weighting info ==========
        if pixel_weights is not None:
            weighted_rows = sum(1 for r in rows if r['weight'] < 1.0)
            if weighted_rows > 0:
                self._logger.info(
                    f"  ✓ Saved radar data to {filename} "
                    f"({len(rows)} rows, {weighted_rows} weighted)"
                )
            else:
                self._logger.info(f"  ✓ Saved radar data to {filename} ({len(rows)} rows)")
        else:
            self._logger.info(f"  ✓ Saved radar data to {filename} ({len(rows)} rows)")
        # ==================================================

        return out_path

    # -------------------------------------------------------------------------
    # Main Collection Methods
    # -------------------------------------------------------------------------

    def collect_catchment_data(
        self,
        catchment: Dict[str, Any],
        start_time: datetime,
        end_time: datetime,
        collection_id: int = DEFAULT_COLLECTION_ID,
        traceset_id: int = DEFAULT_TRACESET_ID,
        save_csv: bool = True,
        pixel_weights: Optional[Dict[Tuple[int, int], float]] = None,
    ) -> Dict[str, Any]:
        """
        Collect radar data for a single catchment.

        Args:
            catchment: Catchment dictionary
            start_time: Start time (UTC)
            end_time: End time (UTC)
            collection_id: Radar collection ID
            traceset_id: Traceset ID
            save_csv: Whether to save data to CSV

        Returns:
            Dictionary with collection results
        """
        catchment_id = safe_int(catchment.get("id"))
        catchment_name = catchment.get("name", "Unknown")

        try:
            # Get pixel mapping
            pixel_indices = self.get_pixel_indices_for_catchment(
                catchment, collection_id
            )

            if not pixel_indices:
                self._logger.warning(f"  No pixels found for catchment {catchment_name}")
                return {
                    "catchment_id": catchment_id,
                    "catchment_name": catchment_name,
                    "pixel_count": 0,
                    "pixel_indices": [],
                    "data_records": 0,
                    "csv_path": None,
                }

            self._logger.info(f"  Found {len(pixel_indices)} pixels")

            # Fetch radar data
            data = self.fetch_radar_data(
                pixel_indices=pixel_indices,
                start_time=start_time,
                end_time=end_time,
                collection_id=collection_id,
                traceset_id=traceset_id,
            )

            # Save to CSV
            csv_path = None
            if save_csv and data:
                # ========== MODIFIED: Pass pixel weights ==========
                csv_path = self.save_catchment_radar_data(
                    catchment, 
                    data,
                    pixel_weights=pixel_weights  # ← ADD THIS
                )
                # ==================================================

            return {
                "catchment_id": catchment_id,
                "catchment_name": catchment_name,
                "pixel_count": len(pixel_indices),
                "pixel_indices": pixel_indices,
                "data_records": len(data),
                "csv_path": str(csv_path) if csv_path else None,
            }

        except Exception as e:
            self._logger.error(f"  Failed to collect data: {e}")
            return {
                "catchment_id": catchment_id,
                "catchment_name": catchment_name,
                "pixel_count": 0,
                "pixel_indices": [],
                "data_records": 0,
                "csv_path": None,
                "error": str(e),
            }

    def collect_all(
        self,
        project_id: int,
        start_time: datetime,
        end_time: datetime,
        catchment_ids: Optional[List[int]] = None,
        asset_type_id: int = DEFAULT_CATCHMENT_ASSET_TYPE_ID,
        force_refresh_pixels: bool = False,
        save_csvs: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Collect radar data for all (or selected) stormwater catchments.

        ... (existing docstring) ...
        """
        self._logger.info("=" * 80)
        self._logger.info("Starting radar data collection for all catchments")
        self._logger.info("=" * 80)
        self._logger.info(f"Project ID: {project_id}")
        self._logger.info(f"Time range: {start_time} to {end_time}")
        self._logger.info(f"Force refresh pixels: {force_refresh_pixels}")
        self._logger.info("Using atomic writes (temp folder strategy)")

        # Setup temporary directory for atomic writes
        self.setup_temp_dir()

        try:
            # Load pixel cache from FINAL location (not temp) unless forced refresh
            if not force_refresh_pixels:
                self._load_pixel_cache()
            else:
                self._logger.info("Forcing pixel mapping refresh (cache will be rebuilt)")

            # Fetch catchments
            catchments = self.get_stormwater_catchments(
                project_id=project_id,
                asset_type_id=asset_type_id,
            )

            self.save_catchments_csv()

            # ========== NEW: Ensure pixel mappings are built ==========
            self._logger.info("")
            self._logger.info("Building pixel mappings for all catchments...")
            
            mappings_to_build = []
            for catchment in catchments:
                catchment_id = safe_int(catchment.get("id"))
                if catchment_id not in self._pixel_cache:
                    mappings_to_build.append(catchment)
            
            if mappings_to_build:
                self._logger.info(f"  Need to fetch mappings for {len(mappings_to_build)} catchments")
                for idx, catchment in enumerate(mappings_to_build, start=1):
                    catchment_id = safe_int(catchment.get("id"))
                    catchment_name = catchment.get("name", "Unknown")
                    
                    self._logger.debug(
                        f"  [{idx}/{len(mappings_to_build)}] {catchment_name}: Getting pixel mappings..."
                    )
                    
                    # This populates self._pixel_cache
                    pixel_indices = self.get_pixel_indices_for_catchment(catchment)
            else:
                self._logger.info("  All pixel mappings already cached")
            
            self._logger.info(f"✓ Pixel mappings ready for {len(self._pixel_cache)} catchments")
            # ==========================================================

            # ========== NEW: Calculate pixel area weights ==========
            self._logger.info("")
            self._logger.info("Calculating pixel area weights for de-duplication...")
            
            # Try geometric weights from API, fallback to simple if fails
            from moata_pipeline.common.spatial_utils import calculate_geometric_pixel_weights_from_api
            
            try:
                self._logger.info("Using geometric intersection (API pixel metadata)...")
                pixel_weights = calculate_geometric_pixel_weights_from_api(
                    catchments=catchments,
                    pixel_mappings=self._pixel_cache,
                    client=self._client,
                    collection_id=self.DEFAULT_COLLECTION_ID
                )
                self._logger.info("✓ Geometric weights calculated successfully")
            except Exception as e:
                self._logger.warning(
                    f"Geometric weighting failed: {e}\n"
                    f"Falling back to simple equal-split method"
                )
                pixel_weights = estimate_pixel_area_weights_simple(
                    catchments=catchments,
                    pixel_mappings=self._pixel_cache
                )
            
            # Save weights for inspection
            weights_file = (self._temp_dir if self._temp_dir else self._base_output_dir) / "pixel_weights.json"
            save_pixel_weights(pixel_weights, weights_file)
            self._logger.info(f"✓ Pixel weights saved to: {weights_file}")
            
            # Log statistics
            weighted_pixels = sum(1 for w in pixel_weights.values() if w < 1.0)
            total_pixel_catchment_pairs = len(pixel_weights)
            self._logger.info(
                f"✓ Weighted {weighted_pixels}/{total_pixel_catchment_pairs} "
                f"pixel-catchment pairs ({100*weighted_pixels/total_pixel_catchment_pairs:.1f}%)"
            )
            # =======================================================

            # Filter to specific catchments if requested
            if catchment_ids:
                catchments = [
                    c for c in catchments
                    if safe_int(c.get("id")) in catchment_ids
                ]
                self._logger.info(f"Filtered to {len(catchments)} specified catchments")

            # Collect data for each catchment

            results: List[Dict[str, Any]] = []

            for idx, catchment in enumerate(catchments, start=1):
                name = catchment.get("name", "Unknown")
                self._logger.info(f"\n[{idx}/{len(catchments)}] {name}")

                # ========== MODIFIED: Pass pixel weights ==========
                result = self.collect_catchment_data(
                    catchment=catchment,
                    start_time=start_time,
                    end_time=end_time,
                    save_csv=save_csvs,
                    pixel_weights=pixel_weights,  # ← ADD THIS
                )
                # ==================================================
                results.append(result)
            

            # Save pixel cache and summary (to temp dir)
            self._save_pixel_cache()
            self._save_collection_summary(results, start_time, end_time)

            # SUCCESS: Move temp to final location
            self.finalize_output()

            successful = len([r for r in results if not r.get("error")])
            failed = len([r for r in results if r.get("error")])

            self._logger.info("")
            self._logger.info("=" * 80)
            self._logger.info("✅ Collection Complete")
            self._logger.info("=" * 80)
            self._logger.info(f"Total catchments: {len(results)}")
            self._logger.info(f"Successful: {successful}")
            self._logger.info(f"Failed: {failed}")
            self._logger.info(f"Output: {self._base_output_dir}")
            self._logger.info("=" * 80)

            return results

        except KeyboardInterrupt:
            # ✅ FIX: User cancelled - DON'T clean up, keep temp data
            self._logger.warning("")
            self._logger.warning("=" * 80)
            self._logger.warning("⚠️  Collection cancelled by user")
            self._logger.warning("=" * 80)

            # Preserve temp data on cancellation
            if self._temp_dir and self._temp_dir.exists():
                self._logger.warning(f"⚠️  Temp data preserved: {self._temp_dir}")
                self._logger.warning(f"    Run again to resume, or manually rename to: {self._base_output_dir}")

            self._logger.warning("✓ Existing data preserved (no changes made)")
            raise

        except Exception as e:
            # ✅ FIX: Error occurred - DON'T clean up temp dir, keep data for recovery
            self._logger.error("")
            self._logger.error("=" * 80)
            self._logger.error(f"❌ Collection failed: {e}")
            self._logger.error("=" * 80)

            # Preserve temp data on error
            if self._temp_dir and self._temp_dir.exists():
                self._logger.error(f"⚠️  Temp data preserved for manual recovery: {self._temp_dir}")
                self._logger.error(f"    You can manually rename it to: {self._base_output_dir}")
                self._logger.error(f"    Command: move {self._temp_dir} {self._base_output_dir}")
            else:
                self._logger.error("⚠️  Temp directory not found - may have been moved/deleted")
                self._logger.error(f"    Expected location: {self._temp_dir}")
                self._logger.error("    Check Windows Recycle Bin or OneDrive Recycle Bin")

            # DON'T call cleanup_temp_dir() - preserve data!
            self._logger.error("✓ Existing data preserved (no changes made)")
            raise

    def _save_collection_summary(
        self,
        results: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
    ) -> Path:
        """
        Save collection summary to JSON.

        Args:
            results: List of collection results
            start_time: Collection start time
            end_time: Collection end time

        Returns:
            Path to saved summary file
        """
        # Get the base dir (could be temp or final)
        base_dir = self._temp_dir if self._temp_dir else self._base_output_dir

        summary = {
            "collection_time": datetime.now(timezone.utc).isoformat(),
            "data_start_time": start_time.isoformat(),
            "data_end_time": end_time.isoformat(),
            "total_catchments": len(results),
            "successful_catchments": len([r for r in results if not r.get("error")]),
            "failed_catchments": len([r for r in results if r.get("error")]),
            "total_pixels": sum(r.get("pixel_count", 0) for r in results),
            "total_data_records": sum(r.get("data_records", 0) for r in results),
            "catchments": results,
        }

        out_path = base_dir / "collection_summary.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)

        self._logger.info(f"✓ Saved collection summary to {out_path}")
        return out_path
