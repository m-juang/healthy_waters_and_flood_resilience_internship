"""
Data Collection Module

Provides collectors for rain gauge and radar QPE (Quantitative Precipitation
Estimation) data from Moata API.

Classes:
    RainGaugeCollector: Collects rain gauge assets, traces, alarms, and thresholds
    RadarDataCollector: Collects radar QPE data for stormwater catchments

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from moata_pipeline.moata.client import MoataClient
from moata_pipeline.common.typing_utils import safe_int
from moata_pipeline.common.time_utils import iso_z
from moata_pipeline.common.text_utils import safe_filename
from moata_pipeline.common.iter_utils import chunk

# Optional: shapely for geometry simplification
try:
    from shapely import wkt as shapely_wkt
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False


# Version info
__version__ = "1.0.0"


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
    
    def __init__(self, client: MoataClient) -> None:
        """
        Initialize rain gauge collector.
        
        Args:
            client: Authenticated MoataClient instance
        """
        if not isinstance(client, MoataClient):
            raise TypeError(
                f"client must be MoataClient instance, got {type(client).__name__}"
            )
        
        self._client = client
        self._logger = logging.getLogger(f"{__name__}.RainGaugeCollector")

    def collect(
        self,
        project_id: int,
        asset_type_id: int,
        trace_batch_size: int = 100,
        fetch_thresholds: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Collect complete rain gauge data including traces, alarms, and thresholds.
        
        Args:
            project_id: Moata project ID
            asset_type_id: Asset type ID for rain gauges (typically 100)
            trace_batch_size: Number of assets to fetch traces for per batch
            fetch_thresholds: Whether to fetch alarm thresholds (slower)
            
        Returns:
            List of dictionaries, each containing:
                - gauge: Asset information
                - traces: List of trace data with alarms and thresholds
                
        Raises:
            ValueError: If project_id or asset_type_id are invalid
            CollectionError: If collection fails
            
        Example:
            >>> data = collector.collect(
            ...     project_id=594,
            ...     asset_type_id=100,
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
        
        self._logger.info("Starting rain gauge collection...")
        self._logger.info(f"  Project ID: {project_id}")
        self._logger.info(f"  Asset Type ID: {asset_type_id}")
        self._logger.info(f"  Trace Batch Size: {trace_batch_size}")
        self._logger.info(f"  Fetch Thresholds: {fetch_thresholds}")
        
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
            all_data = self._enrich_gauges_with_traces(
                asset_ids=asset_ids,
                gauge_by_id=gauge_by_id,
                traces_by_asset=traces_by_asset,
                detailed_by_trace=detailed_by_trace,
                fetch_thresholds=fetch_thresholds,
            )
            
            self._logger.info(f"✓ Collection complete: {len(all_data)} gauges")
            return all_data
            
        except Exception as e:
            self._logger.error(f"Collection failed: {e}")
            raise CollectionError(f"Failed to collect rain gauge data: {e}") from e

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

    def _enrich_gauges_with_traces(
        self,
        asset_ids: List[int],
        gauge_by_id: Dict[int, Dict[str, Any]],
        traces_by_asset: Dict[int, List[Dict[str, Any]]],
        detailed_by_trace: Dict[int, Dict[str, Any]],
        fetch_thresholds: bool,
    ) -> List[Dict[str, Any]]:
        """Enrich each gauge with its traces, alarms, and thresholds."""
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
                enriched_trace = self._enrich_single_trace(
                    trace=trace,
                    detailed_by_trace=detailed_by_trace,
                    fetch_thresholds=fetch_thresholds,
                )
                
                if enriched_trace:
                    traces_out.append(enriched_trace)

            all_data.append({"gauge": gauge, "traces": traces_out})

        return all_data

    def _enrich_single_trace(
        self,
        trace: Dict[str, Any],
        detailed_by_trace: Dict[int, Dict[str, Any]],
        fetch_thresholds: bool,
    ) -> Optional[Dict[str, Any]]:
        """Enrich a single trace with alarms and thresholds."""
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

        if has_alarms:
            try:
                alarms_raw = self._client.get_alarms_for_trace(trace_id_int)
                alarms_split = self._client.split_alarms_by_type(alarms_raw)

                if fetch_thresholds:
                    thresholds = self._client.get_thresholds_for_trace(trace_id_int)
                    
            except Exception as e:
                self._logger.warning(f"  Failed to fetch alarms for trace {trace_id_int}: {e}")

        detailed_alarm = detailed_by_trace.get(trace_id_int)

        return {
            "trace": trace,
            "alarms": alarms_raw,
            "alarms_by_type": alarms_split,
            "detailed_alarm": detailed_alarm,
            "thresholds": thresholds,
        }


# =============================================================================
# Radar Data Collector
# =============================================================================

class RadarDataCollector:
    """
    Collector for radar QPE (Quantitative Precipitation Estimation) data.
    
    Collects spatial rainfall data from radar for stormwater catchments,
    including pixel mapping and timeseries data.
    
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
        """
        Initialize radar data collector.
        
        Args:
            client: Authenticated MoataClient instance
            output_dir: Base output directory
            pixel_batch_size: Number of pixels per API request (1-150, recommend 50)
            max_hours_per_request: Maximum hours per request (1-24)
            
        Raises:
            TypeError: If client is not MoataClient
            ValueError: If batch_size or max_hours are out of range
        """
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
        self._base_output_dir = Path(output_dir) if output_dir else Path("outputs/rain_radar/raw")
        self._pixel_batch_size = pixel_batch_size
        self._max_hours_per_request = max_hours_per_request
        self._logger = logging.getLogger(f"{__name__}.RadarDataCollector")
        
        # Sub-directories
        self._catchments_dir = self._base_output_dir / "catchments"
        self._pixel_mappings_dir = self._base_output_dir / "pixel_mappings"
        self._radar_data_dir = self._base_output_dir / "radar_data"
        
        # Cache for pixel mappings (catchment_id -> [pixel_indices])
        self._pixel_cache: Dict[int, List[int]] = {}
        self._pixel_cache_json = self._pixel_mappings_dir / "catchment_pixel_mapping.json"
        self._pixel_cache_pkl = self._pixel_mappings_dir / "catchment_pixel_mapping.pkl"
        
        # Catchments cache
        self._catchments: List[Dict[str, Any]] = []
        
        self._logger.info("RadarDataCollector initialized")
        self._logger.info(f"  Output directory: {self._base_output_dir}")
        self._logger.info(f"  Pixel batch size: {self._pixel_batch_size}")

    def _ensure_dirs(self) -> None:
        """Create output directories if they don't exist."""
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
                        len(wkt), len(simplified_wkt), current_tolerance, i+1
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
    
    def _load_pixel_cache(self) -> bool:
        """
        Load cached pixel mappings from disk.
        
        Tries pickle first (faster), then JSON (portable).
        
        Returns:
            True if cache was loaded successfully, False otherwise
        """
        # Try pickle first (faster)
        if self._pixel_cache_pkl.exists():
            try:
                with open(self._pixel_cache_pkl, "rb") as f:
                    self._pixel_cache = pickle.load(f)
                self._logger.info(
                    "✓ Loaded pixel cache from pkl: %d catchments",
                    len(self._pixel_cache)
                )
                return True
            except Exception as e:
                self._logger.warning(f"Failed to load pkl cache: {e}")
        
        # Fallback to JSON
        if self._pixel_cache_json.exists():
            try:
                with open(self._pixel_cache_json, "r", encoding="utf-8") as f:
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
            raise ValueError(f"start_time must be before end_time")
        
        start_str = iso_z(start_time)
        end_str = iso_z(end_time)
        
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
    ) -> Optional[Path]:
        """
        Save radar data for a single catchment to CSV.
        
        Args:
            catchment: Catchment dictionary
            data: Radar data from API
            
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
                
                rows.append({
                    "pixel_index": pixel_index,
                    "value_index": i,
                    "timestamp": timestamp.isoformat() if timestamp else None,
                    "value": value,
                })
        
        if not rows:
            self._logger.debug("  No valid rows after processing")
            return None
        
        df = pd.DataFrame(rows)
        out_path = self._radar_data_dir / filename
        df.to_csv(out_path, index=False)
        
        self._logger.info(f"  ✓ Saved radar data to {filename} ({len(rows)} rows)")
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
                csv_path = self.save_catchment_radar_data(catchment, data)
            
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
        
        Args:
            project_id: Moata project ID
            start_time: Start time (UTC)
            end_time: End time (UTC)
            catchment_ids: Optional list of specific catchment IDs to collect
            asset_type_id: Asset type ID for catchments
            force_refresh_pixels: Force rebuild pixel mappings from API
            save_csvs: Save individual catchment CSV files
            
        Returns:
            List of collection result dictionaries
            
        Example:
            >>> results = collector.collect_all(
            ...     project_id=594,
            ...     start_time=datetime(2025, 5, 9, tzinfo=timezone.utc),
            ...     end_time=datetime(2025, 5, 10, tzinfo=timezone.utc),
            ...     catchment_ids=[123, 456]  # Optional: specific catchments
            ... )
        """
        self._ensure_dirs()
        
        self._logger.info("=" * 80)
        self._logger.info("Starting radar data collection for all catchments")
        self._logger.info("=" * 80)
        self._logger.info(f"Project ID: {project_id}")
        self._logger.info(f"Time range: {start_time} to {end_time}")
        self._logger.info(f"Force refresh pixels: {force_refresh_pixels}")
        
        # Load pixel cache unless forced refresh
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
            
            result = self.collect_catchment_data(
                catchment=catchment,
                start_time=start_time,
                end_time=end_time,
                save_csv=save_csvs,
            )
            results.append(result)
        
        # Save pixel cache and summary
        self._save_pixel_cache()
        self._save_collection_summary(results, start_time, end_time)
        
        successful = len([r for r in results if not r.get("error")])
        failed = len([r for r in results if r.get("error")])
        
        self._logger.info("")
        self._logger.info("=" * 80)
        self._logger.info("Collection Complete")
        self._logger.info("=" * 80)
        self._logger.info(f"Total catchments: {len(results)}")
        self._logger.info(f"Successful: {successful}")
        self._logger.info(f"Failed: {failed}")
        self._logger.info("=" * 80)
        
        return results

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
        
        out_path = self._base_output_dir / "collection_summary.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        
        self._logger.info(f"✓ Saved collection summary to {out_path}")
        return out_path