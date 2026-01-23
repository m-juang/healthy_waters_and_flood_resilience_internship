"""
Radar Data Collector Module

Collects radar QPE data for stormwater catchments.

Classes:
    RadarCollector: Orchestrates radar data collection

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 2.0.0 - Refactored into specialized collectors
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from moata_pipeline.moata.client import MoataClient
from moata_pipeline.common.text_utils import safe_filename
from moata_pipeline.common.typing_utils import safe_int
from moata_pipeline.common.time_utils import iso_z

from .catchment_fetcher import CatchmentFetcher
from .pixel_mapper import PixelMapper
from .radar_data_fetcher import RadarDataFetcher
from .weight_calculator import WeightCalculator
from .output_manager import OutputManager


class RadarCollector:
    """
    Orchestrates radar QPE data collection for stormwater catchments.
    
    This class coordinates specialized collectors:
    - CatchmentFetcher: Fetch catchment geometries
    - PixelMapper: Map pixels to catchments
    - RadarDataFetcher: Fetch radar timeseries
    - WeightCalculator: Calculate pixel area weights
    - OutputManager: Save results to files
    
    Uses atomic write strategy: writes to temp folder, then moves to final location
    on success. This prevents partial data corruption on failures.
    
    Args:
        client: Authenticated MoataClient instance
        output_dir: Base output directory (e.g., outputs/rain_radar/20250509-20250510/raw)
        pixel_batch_size: Pixels per API request (default: 50)
        
    Example:
        >>> collector = RadarCollector(client, output_dir)
        >>> results = collector.collect_all(
        ...     project_id=1,
        ...     start_time=datetime(2025, 5, 9, 0, 0, tzinfo=timezone.utc),
        ...     end_time=datetime(2025, 5, 10, 0, 0, tzinfo=timezone.utc)
        ... )
    """
    
    DEFAULT_COLLECTION_ID = 1  # QPE collection
    DEFAULT_TRACESET_ID = 3    # QPE traceset
    DEFAULT_CATCHMENT_ASSET_TYPE_ID = 3541  # Stormwater catchments
    
    def __init__(
        self,
        client: MoataClient,
        output_dir: Path,
        pixel_batch_size: int = 50
    ) -> None:
        """
        Initialize radar collector.
        
        Args:
            client: Authenticated MoataClient instance
            output_dir: Base output directory
            pixel_batch_size: Pixels per API request (1-150)
        """
        self._client = client
        self._base_output_dir = Path(output_dir)
        self._temp_dir: Optional[Path] = None
        
        # Initialize specialized collectors
        self._catchment_fetcher = CatchmentFetcher(client)
        self._pixel_mapper = PixelMapper(client)
        self._radar_data_fetcher = RadarDataFetcher(client, pixel_batch_size)
        self._weight_calculator = WeightCalculator(client)
        self._output_manager = OutputManager(output_dir)
        
        self._logger = logging.getLogger(f"{__name__}.RadarCollector")
    
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
            start_time: Start of data collection period (UTC)
            end_time: End of data collection period (UTC)
            catchment_ids: Optional list of specific catchment IDs
            asset_type_id: Catchment asset type ID (default: 3541)
            force_refresh_pixels: Force rebuild of pixel mappings cache
            save_csvs: Save individual catchment CSV files
        Returns:
            List of collection result dictionaries
        Example:
            >>> results = collector.collect_all(
            ...     project_id=1,
            ...     start_time=datetime(2025, 5, 9, 0, 0, tzinfo=timezone.utc),
            ...     end_time=datetime(2025, 5, 10, 0, 0, tzinfo=timezone.utc),
            ...     catchment_ids=[1, 2, 3],  # Optional filter
            ...     force_refresh_pixels=False
            ... )
        """
        self._logger.info("=" * 80)
        self._logger.info("Starting radar data collection")
        self._logger.info("=" * 80)
        self._logger.info(f"Project ID: {project_id}")
        self._logger.info(f"Time range: {iso_z(start_time)} to {iso_z(end_time)}")
        self._logger.info(f"Force refresh pixels: {force_refresh_pixels}")
        self._logger.info("Using atomic writes (temp folder strategy)")
        # Setup temporary directory for atomic writes
        self._setup_temp_dir()
        try:
            # Step 1: Fetch catchments
            self._logger.info("")
            self._logger.info("STEP 1: Fetching catchments...")
            catchments = self._catchment_fetcher.fetch_catchments(
                project_id=project_id,
                asset_type_id=asset_type_id
            )
            self._logger.info(f"✓ Fetched {len(catchments)} catchments")
            # Step 2: Build pixel mappings
            self._logger.info("")
            self._logger.info("STEP 2: Building pixel mappings...")
            pixel_mappings = self._pixel_mapper.build_pixel_mappings(
                catchments=catchments,
                collection_id=self.DEFAULT_COLLECTION_ID,
                force_refresh=force_refresh_pixels
            )
            unique_pixels = self._pixel_mapper.get_unique_pixels(pixel_mappings)
            total_pairs = self._pixel_mapper.count_total_pixels(pixel_mappings)
            self._logger.info(f"✓ Mapped {len(unique_pixels)} unique pixels across {total_pairs} pixel-catchment pairs")
            # Step 3: Calculate pixel weights
            self._logger.info("")
            self._logger.info("STEP 3: Calculating pixel area weights...")
            pixel_weights = self._weight_calculator.calculate_weights(
                catchments=catchments,
                pixel_mappings=pixel_mappings,
                output_dir=self._temp_dir
            )
            # Step 4: Filter catchments if requested
            if catchment_ids:
                original_count = len(catchments)
                catchments = [
                    c for c in catchments
                    if safe_int(c.get("id")) in catchment_ids
                ]
                self._logger.info("")
                self._logger.info(f"Filtered to {len(catchments)}/{original_count} specified catchments")
            # Step 5: Collect data for each catchment
            self._logger.info("")
            self._logger.info(f"STEP 4: Collecting radar data for {len(catchments)} catchments...")
            results = self._collect_catchments_data(
                catchments=catchments,
                pixel_mappings=pixel_mappings,
                pixel_weights=pixel_weights,
                start_time=start_time,
                end_time=end_time,
                save_csvs=save_csvs
            )
            # Step 6: Save summary
            self._logger.info("")
            self._logger.info("STEP 5: Saving collection summary...")
            self._save_collection_summary(results, start_time, end_time)
            # SUCCESS: Move temp to final location
            self._finalize_output()
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
            self._logger.warning("")
            self._logger.warning("=" * 80)
            self._logger.warning("⚠️  Collection cancelled by user")
            self._logger.warning("=" * 80)
            if self._temp_dir and self._temp_dir.exists():
                self._logger.warning(f"⚠️  Temp data preserved: {self._temp_dir}")
                self._logger.warning(f"    Manually rename to: {self._base_output_dir}")
            self._logger.warning("✓ Existing data preserved (no changes made)")
            raise
        except Exception as e:
            self._logger.error("")
            self._logger.error("=" * 80)
            self._logger.error(f"❌ Collection failed: {e}")
            self._logger.error("=" * 80)
            if self._temp_dir and self._temp_dir.exists():
                self._logger.error(f"⚠️  Temp data preserved: {self._temp_dir}")
                self._logger.error(f"    Manually rename to: {self._base_output_dir}")
            self._logger.error("✓ Existing data preserved (no changes made)")
            raise

    def _write_catchment_metadata(self, catchment: dict, pixel_indices: list) -> None:
        """
        Write per-catchment metadata JSON to the catchments/ folder.
        Includes id, name, geometry, and pixel_indices.
        """
        import json
        catchment_id = safe_int(catchment.get("id"))
        name = safe_filename(catchment.get("name", "unknown"))
        geometry = catchment.get("geometry")
        meta = {
            "id": catchment_id,
            "name": name,
            "geometry": geometry,
            "pixel_indices": pixel_indices,
        }
        # Determine output path (temp dir if atomic, else base dir)
        base_dir = self._temp_dir if self._temp_dir else self._base_output_dir
        out_dir = base_dir / "catchments"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{catchment_id}_{name}.json"
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
            self._logger.info(f"  ✓ Wrote catchment metadata: {out_path.name}")
        except Exception as e:
            self._logger.warning(f"  Failed to write catchment metadata for {name}: {e}")
    
    def _collect_catchments_data(
        self,
        catchments: List[Dict[str, Any]],
        pixel_mappings: Dict[int, List[int]],
        pixel_weights: Dict[Tuple[int, int], float],
        start_time: datetime,
        end_time: datetime,
        save_csvs: bool
    ) -> List[Dict[str, Any]]:
        """
        Collect radar data for multiple catchments.
        
        Args:
            catchments: List of catchment dictionaries
            pixel_mappings: Pixel mappings per catchment
            pixel_weights: Pixel area weights
            start_time: Data start time
            end_time: Data end time
            save_csvs: Save individual CSV files
            
        Returns:
            List of result dictionaries
        """
        results = []
        
        for idx, catchment in enumerate(catchments, start=1):
            catchment_id = safe_int(catchment.get("id"))
            name = catchment.get("name", "Unknown")
            self._logger.info(f"\n[{idx}/{len(catchments)}] {name}")
            try:
                # Get pixel indices for this catchment
                pixel_indices = pixel_mappings.get(catchment_id, [])
                # Always write metadata file for every catchment
                self._write_catchment_metadata(catchment, pixel_indices)
                if not pixel_indices:
                    self._logger.warning(f"  No pixels found for catchment {name}")
                    results.append({
                        "catchment_id": catchment_id,
                        "catchment_name": name,
                        "pixel_count": 0,
                        "pixel_indices": [],
                        "data_records": 0,
                        "csv_path": None,
                    })
                    continue
                self._logger.info(f"  Found {len(pixel_indices)} pixels")
                # Fetch radar data
                radar_df = self._radar_data_fetcher.fetch_radar_data(
                    pixels=pixel_indices,
                    start_time=start_time,
                    end_time=end_time,
                    collection_id=self.DEFAULT_COLLECTION_ID,
                    traceset_id=self.DEFAULT_TRACESET_ID
                )
                # Save to CSV
                csv_path = None
                if save_csvs and not radar_df.empty:
                    csv_path = self._save_catchment_data(
                        catchment=catchment,
                        radar_df=radar_df,
                        pixel_weights=pixel_weights
                    )
                results.append({
                    "catchment_id": catchment_id,
                    "catchment_name": name,
                    "pixel_count": len(pixel_indices),
                    "pixel_indices": pixel_indices,
                    "data_records": len(radar_df),
                    "csv_path": str(csv_path) if csv_path else None,
                })
            except Exception as e:
                self._logger.error(f"  Failed to collect data: {e}")
                # Still write metadata file for catchment with empty pixel_indices
                self._write_catchment_metadata(catchment, [])
                results.append({
                    "catchment_id": catchment_id,
                    "catchment_name": name,
                    "pixel_count": 0,
                    "pixel_indices": [],
                    "data_records": 0,
                    "csv_path": None,
                    "error": str(e),
                })
        
        return results
    
    def _save_catchment_data(
        self,
        catchment: Dict[str, Any],
        radar_df: Any,  # pandas DataFrame
        pixel_weights: Dict[Tuple[int, int], float]
    ) -> Optional[Path]:
        """
        Save radar data for catchment to CSV.
        
        Args:
            catchment: Catchment dictionary
            radar_df: Radar data DataFrame
            pixel_weights: Pixel area weights
            
        Returns:
            Path to saved CSV, or None if no data
        """
        if radar_df.empty:
            return None
        
        catchment_id = safe_int(catchment.get("id"))
        name = safe_filename(catchment.get("name", "unknown"))
        filename = f"{catchment_id}_{name}.csv"
        
        output_path = (self._temp_dir / "radar_data" / filename) if self._temp_dir else (self._base_output_dir / "radar_data" / filename)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Apply weights if available
        rows = []
        for _, row in radar_df.iterrows():
            pixel_idx = row["pixel_index"]
            weight = pixel_weights.get((catchment_id, pixel_idx), 1.0)
            
            rows.append({
                "timestamp": row["timestamp"],
                "pixel_index": pixel_idx,
                "value": row["value"],
                "weight": weight,
                "weighted_value": row["value"] * weight
            })
        
        # Save to CSV
        import pandas as pd
        weighted_df = pd.DataFrame(rows)
        weighted_df.to_csv(output_path, index=False)
        
        self._logger.info(f"  ✓ Saved {len(weighted_df)} records to {filename}")
        return output_path
    
    def _setup_temp_dir(self) -> None:
        """Setup temporary directory for atomic writes."""
        self._temp_dir = self._base_output_dir.parent / "_temp_raw"
        
        if self._temp_dir.exists():
            shutil.rmtree(self._temp_dir)
        
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        (self._temp_dir / "catchments").mkdir(exist_ok=True)
        (self._temp_dir / "radar_data").mkdir(exist_ok=True)
        
        self._logger.debug(f"✓ Temp directory ready: {self._temp_dir}")
    
    def _finalize_output(self) -> None:
        """Move data from temp directory to final location."""
        if not self._temp_dir or not self._temp_dir.exists():
            self._logger.warning("No temp directory to finalize")
            return
        
        self._logger.info("Finalizing output (moving from temp to final location)...")
        
        # Remove existing output if present
        if self._base_output_dir.exists():
            try:
                shutil.rmtree(self._base_output_dir)
            except PermissionError as e:
                # Rename locked folder
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                old_dir = self._base_output_dir.parent / f"raw_old_{timestamp}"
                self._base_output_dir.rename(old_dir)
                self._logger.warning(f"Renamed locked folder to: {old_dir}")
        
        # Move temp to final
        self._temp_dir.rename(self._base_output_dir)
        self._logger.info(f"✓ Output finalized: {self._base_output_dir}")
    
    def _save_collection_summary(
        self,
        results: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Path:
        """
        Save collection summary to JSON.
        
        Args:
            results: Collection results
            start_time: Data start time
            end_time: Data end time
            
        Returns:
            Path to summary file
        """
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
        
        self._logger.info(f"✓ Saved collection summary")
        return out_path
