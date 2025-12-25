from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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

logger = logging.getLogger(__name__)


# =============================================================================
# Rain Gauge Collector
# =============================================================================

class RainGaugeCollector:
    """Collector for rain gauge data with traces and alarms."""
    
    def __init__(self, client: MoataClient) -> None:
        self._client = client

    def collect(
        self,
        project_id: int,
        asset_type_id: int,
        trace_batch_size: int = 100,
        fetch_thresholds: bool = True,
    ) -> List[Dict[str, Any]]:
        # 1) Assets
        gauges = self._client.get_rain_gauges(project_id=project_id, asset_type_id=asset_type_id)
        logger.info("✓ Fetched %d rain gauges", len(gauges))

        # 2) Project-level detailed alarms (map by traceId)
        detailed_by_trace = self._client.get_detailed_alarms_by_project(project_id=project_id)
        logger.info("✓ Fetched %d detailed alarms (project-level)", len(detailed_by_trace))

        # Gather asset ids
        asset_ids: List[int] = []
        gauge_by_asset_id: Dict[int, Dict[str, Any]] = {}

        for g in gauges:
            asset_id = g.get("id") or g.get("assetId")
            asset_id_int = safe_int(asset_id)
            if asset_id_int is None:
                logger.warning("Gauge without valid id: %s", g)
                continue
            asset_ids.append(asset_id_int)
            gauge_by_asset_id[asset_id_int] = g

        if not asset_ids:
            return []

        # 3) Traces (BATCH)
        all_traces: List[Dict[str, Any]] = []
        for batch in chunk(asset_ids, trace_batch_size):
            traces = self._client.get_traces_for_assets(batch)
            all_traces.extend(traces)

        logger.info("✓ Fetched %d traces (batched)", len(all_traces))

        # Group traces by assetId
        traces_by_asset: Dict[int, List[Dict[str, Any]]] = {}
        for t in all_traces:
            aid = safe_int(t.get("assetId"))
            if aid is None:
                continue
            traces_by_asset.setdefault(aid, []).append(t)

        # 4) For each gauge/asset, enrich traces with alarms/thresholds
        all_data: List[Dict[str, Any]] = []

        for idx, asset_id in enumerate(asset_ids, start=1):
            g = gauge_by_asset_id.get(asset_id, {})
            name = g.get("name", "Unknown")

            logger.info("Processing %d/%d: %s (asset_id=%s)", idx, len(asset_ids), name, asset_id)

            traces = traces_by_asset.get(asset_id, [])
            traces_out: List[Dict[str, Any]] = []

            for t in traces:
                trace_id = t.get("id") or t.get("traceId")
                trace_id_int = safe_int(trace_id)
                if trace_id_int is None:
                    continue

                has_alarms = bool(t.get("hasAlarms", False))

                alarms_raw: List[Dict[str, Any]] = []
                alarms_split: Dict[str, List[Dict[str, Any]]] = {"overflow": [], "recency": [], "other": []}
                thresholds: List[Dict[str, Any]] = []

                if has_alarms:
                    alarms_raw = self._client.get_alarms_for_trace(trace_id_int)
                    alarms_split = self._client.split_alarms_by_type(alarms_raw)

                    if fetch_thresholds:
                        thresholds = self._client.get_thresholds_for_trace(trace_id_int)

                detailed_alarm = detailed_by_trace.get(trace_id_int)

                traces_out.append(
                    {
                        "trace": t,
                        "alarms": alarms_raw,
                        "alarms_by_type": alarms_split,
                        "detailed_alarm": detailed_alarm,
                        "thresholds": thresholds,
                    }
                )

            all_data.append({"gauge": g, "traces": traces_out})

        return all_data


# =============================================================================
# Radar Data Collector
# =============================================================================

class RadarDataCollector:
    """
    Collector for radar QPE (Quantitative Precipitation Estimate) data.
    
    Output structure:
        outputs/rain_radar/raw/
        ├── catchments/
        │   └── stormwater_catchments.csv
        ├── pixel_mappings/
        │   ├── catchment_pixel_mapping.json
        │   └── catchment_pixel_mapping.pkl
        ├── radar_data/
        │   └── {catchment_id}_{catchment_name}.csv
        └── collection_summary.json
    
    Notes from Sam:
    - Use collection_id=1, traceset_id=3 for QPE data
    - Pixel indices don't change, can be cached
    - Max 150 pixels per request (recommend 50)
    - Max 24 hours of data per request
    - Data is minute resolution per pixel
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
        self._client = client
        self._base_output_dir = Path(output_dir) if output_dir else Path("outputs/rain_radar/raw")
        self._pixel_batch_size = pixel_batch_size
        self._max_hours_per_request = max_hours_per_request
        
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

    def _ensure_dirs(self) -> None:
        """Create output directories if they don't exist."""
        self._catchments_dir.mkdir(parents=True, exist_ok=True)
        self._pixel_mappings_dir.mkdir(parents=True, exist_ok=True)
        self._radar_data_dir.mkdir(parents=True, exist_ok=True)

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
        """
        if not SHAPELY_AVAILABLE:
            logger.warning("Shapely not available, cannot simplify geometry")
            return wkt
        
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
                    logger.info(
                        "    Simplified geometry: %d -> %d chars (tolerance=%.6f)",
                        len(wkt), len(simplified_wkt), current_tolerance
                    )
                    return simplified_wkt
                
                current_tolerance *= 2
            
            # Last resort: use convex hull
            hull = geom.convex_hull
            hull_wkt = hull.wkt
            logger.warning("    Using convex hull: %d -> %d chars", len(wkt), len(hull_wkt))
            return hull_wkt
            
        except Exception as e:
            logger.error("Failed to simplify geometry: %s", e)
            return wkt

    # -------------------------------------------------------------------------
    # Pixel Cache Management
    # -------------------------------------------------------------------------
    def _load_pixel_cache(self) -> bool:
        """Load cached pixel mappings. Returns True if loaded successfully."""
        if self._pixel_cache_pkl.exists():
            try:
                with open(self._pixel_cache_pkl, "rb") as f:
                    self._pixel_cache = pickle.load(f)
                logger.info("✓ Loaded pixel cache from pkl: %d catchments", len(self._pixel_cache))
                return True
            except Exception as e:
                logger.warning("Failed to load pkl cache: %s", e)
        
        if self._pixel_cache_json.exists():
            try:
                with open(self._pixel_cache_json, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._pixel_cache = {int(k): v for k, v in data.items()}
                logger.info("✓ Loaded pixel cache from json: %d catchments", len(self._pixel_cache))
                return True
            except Exception as e:
                logger.warning("Failed to load json cache: %s", e)
        
        return False

    def _save_pixel_cache(self) -> None:
        """Save pixel mappings to both JSON and pickle."""
        self._ensure_dirs()
        
        try:
            with open(self._pixel_cache_json, "w", encoding="utf-8") as f:
                json.dump(self._pixel_cache, f, indent=2)
            logger.info("✓ Saved pixel cache to json: %d catchments", len(self._pixel_cache))
        except Exception as e:
            logger.warning("Failed to save json cache: %s", e)
        
        try:
            with open(self._pixel_cache_pkl, "wb") as f:
                pickle.dump(self._pixel_cache, f)
            logger.info("✓ Saved pixel cache to pkl")
        except Exception as e:
            logger.warning("Failed to save pkl cache: %s", e)

    # -------------------------------------------------------------------------
    # Catchments
    # -------------------------------------------------------------------------
    def get_stormwater_catchments(
        self,
        project_id: int,
        asset_type_id: int = DEFAULT_CATCHMENT_ASSET_TYPE_ID,
        sr_id: int = 4326,
    ) -> List[Dict[str, Any]]:
        """Get stormwater catchment assets with geometry."""
        logger.info(
            "Fetching stormwater catchments (project=%d, assetType=%d)...",
            project_id, asset_type_id
        )
        
        catchments = self._client.get_assets_with_geometry(
            project_id=project_id,
            asset_type_id=asset_type_id,
            sr_id=sr_id,
        )
        
        self._catchments = catchments
        logger.info("✓ Fetched %d stormwater catchments", len(catchments))
        return catchments

    def save_catchments_csv(self) -> Path:
        """Save catchments to CSV."""
        self._ensure_dirs()
        
        if not self._catchments:
            logger.warning("No catchments to save")
            return self._catchments_dir / "stormwater_catchments.csv"
        
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
        
        logger.info("✓ Saved catchments to %s", out_path)
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
        """Get radar pixel indices that intersect a catchment geometry."""
        catchment_id = safe_int(catchment.get("id"))
        if catchment_id is None:
            logger.warning("Catchment without valid ID")
            return []
        
        if use_cache and catchment_id in self._pixel_cache:
            return self._pixel_cache[catchment_id]
        
        wkt = catchment.get("geometryWkt", "")
        if not wkt:
            logger.warning("Catchment %d has no geometry", catchment_id)
            return []
        
        if len(wkt) > self.MAX_WKT_LENGTH:
            logger.info("  Geometry too large (%d chars), simplifying...", len(wkt))
            wkt = self._simplify_wkt(wkt, max_length=self.MAX_WKT_LENGTH)
            
            if len(wkt) > self.MAX_WKT_LENGTH:
                logger.error("  Cannot simplify geometry enough, skipping")
                return []
        
        mappings = self._client.get_pixel_mappings_for_geometry(
            collection_id=collection_id,
            wkt=wkt,
            sr_id=4326,
        )
        
        pixel_indices = [m.get("pixelIndex") for m in mappings if m.get("pixelIndex") is not None]
        self._pixel_cache[catchment_id] = pixel_indices
        
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
        """Fetch radar data for given pixels and time range."""
        if not pixel_indices:
            return []
        
        start_str = iso_z(start_time)
        end_str = iso_z(end_time)
        
        logger.info(
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
        
        logger.info("  ✓ Fetched data for %d pixel-traceset combinations", len(data))
        return data

    def save_catchment_radar_data(
        self,
        catchment: Dict[str, Any],
        data: List[Dict[str, Any]],
    ) -> Optional[Path]:
        """Save radar data for a single catchment to CSV."""
        self._ensure_dirs()
        
        if not data:
            return None
        
        cid = safe_int(catchment.get("id"))
        name = safe_filename(catchment.get("name", "unknown"))
        filename = f"{cid}_{name}.csv"
        
        rows = []
        for d in data:
            pixel_index = d.get("pixelIndex")
            start_time = d.get("startTime")
            offset_seconds = d.get("dataOffsetSeconds", 60)
            values = d.get("values", [])
            
            if not values:
                continue
            
            try:
                if start_time:
                    if start_time.endswith("Z"):
                        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    else:
                        start_dt = datetime.fromisoformat(start_time)
                else:
                    start_dt = None
            except Exception:
                start_dt = None
            
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
            return None
        
        df = pd.DataFrame(rows)
        out_path = self._radar_data_dir / filename
        df.to_csv(out_path, index=False)
        
        logger.info("  ✓ Saved radar data to %s (%d rows)", filename, len(rows))
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
        """Collect radar data for a single catchment."""
        catchment_id = safe_int(catchment.get("id"))
        catchment_name = catchment.get("name", "Unknown")
        
        pixel_indices = self.get_pixel_indices_for_catchment(catchment, collection_id)
        
        if not pixel_indices:
            logger.warning("  No pixels found for catchment %s", catchment_name)
            return {
                "catchment_id": catchment_id,
                "catchment_name": catchment_name,
                "pixel_count": 0,
                "pixel_indices": [],
                "data_records": 0,
                "csv_path": None,
            }
        
        logger.info("  Found %d pixels", len(pixel_indices))
        
        data = self.fetch_radar_data(
            pixel_indices=pixel_indices,
            start_time=start_time,
            end_time=end_time,
            collection_id=collection_id,
            traceset_id=traceset_id,
        )
        
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
        """Collect radar data for all (or selected) stormwater catchments."""
        self._ensure_dirs()
        
        if not force_refresh_pixels:
            self._load_pixel_cache()
        
        catchments = self.get_stormwater_catchments(
            project_id=project_id,
            asset_type_id=asset_type_id,
        )
        
        self.save_catchments_csv()
        
        if catchment_ids:
            catchments = [c for c in catchments if safe_int(c.get("id")) in catchment_ids]
            logger.info("Filtered to %d catchments", len(catchments))
        
        results: List[Dict[str, Any]] = []
        
        for idx, catchment in enumerate(catchments, start=1):
            name = catchment.get("name", "Unknown")
            logger.info("\n[%d/%d] %s", idx, len(catchments), name)
            
            try:
                result = self.collect_catchment_data(
                    catchment=catchment,
                    start_time=start_time,
                    end_time=end_time,
                    save_csv=save_csvs,
                )
                results.append(result)
            except Exception as e:
                logger.error("Failed to collect data for %s: %s", name, e)
                results.append({
                    "catchment_id": safe_int(catchment.get("id")),
                    "catchment_name": name,
                    "pixel_count": 0,
                    "pixel_indices": [],
                    "data_records": 0,
                    "csv_path": None,
                    "error": str(e),
                })
        
        self._save_pixel_cache()
        self._save_collection_summary(results, start_time, end_time)
        
        logger.info("\n✓ Collected data for %d catchments", len(results))
        return results

    def _save_collection_summary(
        self,
        results: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
    ) -> Path:
        """Save collection summary to JSON."""
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
        
        logger.info("✓ Saved collection summary to %s", out_path)
        return out_path