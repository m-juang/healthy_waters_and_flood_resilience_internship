"""
Pixel Mapper Module

Maps radar grid pixels to catchment geometries.

Classes:
    PixelMapper: Maps pixels to catchments and manages pixel cache

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 1.0.0 - Extracted from RadarDataCollector
"""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from moata_pipeline.moata.client import MoataClient


class PixelMapper:
    """
    Maps radar grid pixels to catchment geometries.
    
    Responsible for:
    - Fetching pixel indices from radar grid
    - Mapping pixels to catchments
    - Caching pixel mappings for reuse
    - Loading cached mappings
    
    Args:
        client: Authenticated MoataClient instance
        cache_dir: Directory for pixel cache files
        
    Example:
        >>> mapper = PixelMapper(client, cache_dir=Path("outputs/radar/cache"))
        >>> mappings = mapper.build_pixel_mappings(
        ...     catchments=catchments,
        ...     collection_id=1,
        ...     force_refresh=False
        ... )
    """
    
    DEFAULT_COLLECTION_ID = 1  # Sam's recommended collection ID for QPE
    MAX_WKT_LENGTH = 14000  # Conservative URL length limit
    
    def __init__(
        self,
        client: MoataClient,
        cache_dir: Optional[Path] = None
    ) -> None:
        """
        Initialize pixel mapper.
        
        Args:
            client: Authenticated MoataClient instance
            cache_dir: Directory for cache files (default: None, no caching)
        """
        self._client = client
        self._cache_dir = cache_dir
        self._logger = logging.getLogger(f"{__name__}.PixelMapper")
    
    def build_pixel_mappings(
        self,
        catchments: List[Dict[str, Any]],
        collection_id: int = DEFAULT_COLLECTION_ID,
        force_refresh: bool = False,
    ) -> Dict[int, List[int]]:
        """
        Build pixel mappings for all catchments.
        
        Args:
            catchments: List of catchment dictionaries with geometries
            collection_id: Radar collection ID (default: 1 for QPE)
            force_refresh: Force rebuild even if cache exists
            
        Returns:
            Dictionary mapping catchment_id -> list of pixel indices
            
        Example:
            >>> mappings = mapper.build_pixel_mappings(catchments)
            >>> pixels = mappings[123]  # Get pixels for catchment 123
            >>> print(f"Catchment has {len(pixels)} pixels")
        """
        # Try to load from cache first
        if not force_refresh and self._cache_dir:
            cached_mappings = self._load_pixel_cache()
            if cached_mappings:
                return cached_mappings
        
        self._logger.info("")
        self._logger.info("Building pixel mappings for all catchments...")
        
        pixel_mappings = {}
        total_pixels = 0
        
        for catchment in catchments:
            catchment_id = catchment["id"]
            catchment_name = catchment["name"]
            geometry_wkt = catchment["geometry"]
            
            # Fetch pixels for this catchment
            pixels = self._fetch_pixels_for_geometry(
                geometry_wkt=geometry_wkt,
                collection_id=collection_id,
                catchment_name=catchment_name
            )
            
            pixel_mappings[catchment_id] = pixels
            total_pixels += len(pixels)
        
        self._logger.info(f"✓ Pixel mappings ready for {len(catchments)} catchments")
        self._logger.info(f"  Total unique pixels mapped: {total_pixels}")
        
        # Save to cache
        if self._cache_dir:
            self._save_pixel_cache(pixel_mappings)
        
        return pixel_mappings
    
    def _fetch_pixels_for_geometry(
        self,
        geometry_wkt: str,
        collection_id: int,
        catchment_name: str
    ) -> List[int]:
        """
        Fetch pixel indices for a catchment geometry.
        
        Args:
            geometry_wkt: WKT geometry string
            collection_id: Radar collection ID (traceset_id)
            catchment_name: Catchment name (for logging)
            
        Returns:
            List of pixel indices
        """
        # Check if geometry is too long for URL
        if len(geometry_wkt) > self.MAX_WKT_LENGTH:
            self._logger.warning(
                f"Geometry too large for {catchment_name}, simplifying..."
            )
            geometry_wkt = self._simplify_geometry(geometry_wkt)
        
        # Fetch pixel indices from API
        try:
            # Use the correct method name: get_pixel_mappings_for_geometry
            # Parameters: traceset_id (same as collection_id), geometry (WKT string), sr_id
            pixel_mappings = self._client.get_pixel_mappings_for_geometry(
                traceset_id=collection_id,
                geometry=geometry_wkt,
                sr_id=4326
            )
            
            # Extract pixel indices from the mappings
            # API returns list of dicts with 'pixelIndex' key
            pixels = [
                pm.get("pixelIndex") 
                for pm in pixel_mappings 
                if pm.get("pixelIndex") is not None
            ]
            
            self._logger.debug(f"  {catchment_name}: {len(pixels)} pixels")
            return pixels
            
        except Exception as e:
            self._logger.error(
                f"Failed to fetch pixels for {catchment_name}: {e}"
            )
            return []
    
    def _simplify_geometry(self, geometry_wkt: str) -> str:
        """
        Simplify geometry if it's too large for URL.
        
        Args:
            geometry_wkt: Original WKT geometry
            
        Returns:
            Simplified WKT geometry
        """
        try:
            from shapely import wkt as shapely_wkt
            from shapely.geometry import Polygon
            
            geom = shapely_wkt.loads(geometry_wkt)
            
            # Simplify with increasing tolerance until small enough
            for tolerance in [0.0001, 0.0005, 0.001, 0.005]:
                simplified = geom.simplify(tolerance)
                simplified_wkt = shapely_wkt.dumps(simplified)
                
                if len(simplified_wkt) <= self.MAX_WKT_LENGTH:
                    self._logger.debug(
                        f"Simplified geometry: {len(geometry_wkt)} → {len(simplified_wkt)} chars"
                    )
                    return simplified_wkt
            
            # If still too large, use envelope (bounding box)
            envelope = geom.envelope
            envelope_wkt = shapely_wkt.dumps(envelope)
            self._logger.warning("Using envelope (bounding box) for geometry")
            return envelope_wkt
            
        except ImportError:
            self._logger.warning("shapely not available, using original geometry")
            return geometry_wkt
        except Exception as e:
            self._logger.warning(f"Geometry simplification failed: {e}")
            return geometry_wkt
    
    def _load_pixel_cache(self) -> Optional[Dict[int, List[int]]]:
        """
        Load pixel mappings from cache.
        
        Returns:
            Cached pixel mappings or None if not found
        """
        if not self._cache_dir:
            return None
        
        pkl_file = self._cache_dir / "pixels.pkl"
        json_file = self._cache_dir / "pixels.json"
        
        # Try pickle first (faster)
        if pkl_file.exists():
            try:
                with open(pkl_file, "rb") as f:
                    mappings = pickle.load(f)
                self._logger.info(f"✓ Loaded pixel cache from pkl: {len(mappings)} catchments")
                return mappings
            except Exception as e:
                self._logger.warning(f"Failed to load pkl cache: {e}")
        
        # Try JSON as fallback
        if json_file.exists():
            try:
                with open(json_file, "r") as f:
                    data = json.load(f)
                # Convert string keys back to int
                mappings = {int(k): v for k, v in data.items()}
                self._logger.info(f"✓ Loaded pixel cache from json: {len(mappings)} catchments")
                return mappings
            except Exception as e:
                self._logger.warning(f"Failed to load json cache: {e}")
        
        return None
    
    def _save_pixel_cache(self, mappings: Dict[int, List[int]]) -> None:
        """
        Save pixel mappings to cache.
        
        Args:
            mappings: Pixel mappings to save
        """
        if not self._cache_dir:
            return
        
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as JSON (human-readable)
        json_file = self._cache_dir / "pixels.json"
        try:
            with open(json_file, "w") as f:
                json.dump(mappings, f, indent=2)
            self._logger.info(f"✓ Saved pixel cache to json: {len(mappings)} catchments")
        except Exception as e:
            self._logger.warning(f"Failed to save json cache: {e}")
        
        # Save as pickle (faster loading)
        pkl_file = self._cache_dir / "pixels.pkl"
        try:
            with open(pkl_file, "wb") as f:
                pickle.dump(mappings, f)
            self._logger.info(f"✓ Saved pixel cache to pkl")
        except Exception as e:
            self._logger.warning(f"Failed to save pkl cache: {e}")
    
    def get_unique_pixels(
        self,
        mappings: Dict[int, List[int]]
    ) -> List[int]:
        """
        Get list of all unique pixel indices across all catchments.
        
        Args:
            mappings: Pixel mappings dictionary
            
        Returns:
            Sorted list of unique pixel indices
            
        Example:
            >>> unique = mapper.get_unique_pixels(mappings)
            >>> print(f"Total unique pixels: {len(unique)}")
        """
        all_pixels = set()
        for pixels in mappings.values():
            all_pixels.update(pixels)
        
        return sorted(all_pixels)
    
    def count_total_pixels(
        self,
        mappings: Dict[int, List[int]]
    ) -> int:
        """
        Count total pixel-catchment pairs.
        
        Args:
            mappings: Pixel mappings dictionary
            
        Returns:
            Total count of pixel-catchment pairs
        """
        return sum(len(pixels) for pixels in mappings.values())
