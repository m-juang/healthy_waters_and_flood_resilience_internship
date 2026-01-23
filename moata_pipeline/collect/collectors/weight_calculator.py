"""
Weight Calculator Module

Calculates pixel area weights for de-duplication when pixels overlap multiple catchments.

Classes:
    WeightCalculator: Calculates and saves pixel area weights

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 1.0.0 - Extracted from RadarDataCollector
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from moata_pipeline.moata.client import MoataClient
from moata_pipeline.common.spatial_utils import (
    calculate_geometric_pixel_weights_from_api,
    estimate_pixel_area_weights_simple,
    save_pixel_weights,
)


class WeightCalculator:
    """
    Calculates pixel area weights for de-duplication.
    
    When a radar pixel overlaps multiple catchments, we need to weight
    contributions by the intersection area to avoid double-counting.
    
    Strategies:
    1. Geometric (preferred): Uses actual pixel geometries from API
    2. Simple (fallback): Equal split across overlapping catchments
    
    Args:
        client: Authenticated MoataClient instance
        collection_id: Radar collection ID (default: 1)
        
    Example:
        >>> calculator = WeightCalculator(client)
        >>> weights = calculator.calculate_weights(
        ...     catchments=catchments,
        ...     pixel_mappings=pixel_mappings,
        ...     output_dir=Path("outputs/rain_radar/20250509-20250510/raw")
        ... )
        >>> # weights: Dict[(catchment_id, pixel_index), weight]
    """
    
    DEFAULT_COLLECTION_ID = 1  # QPE collection
    
    def __init__(
        self,
        client: MoataClient,
        collection_id: int = DEFAULT_COLLECTION_ID
    ) -> None:
        """
        Initialize weight calculator.
        
        Args:
            client: Authenticated MoataClient instance
            collection_id: Radar collection ID (default: 1)
        """
        self._client = client
        self._collection_id = collection_id
        self._logger = logging.getLogger(f"{__name__}.WeightCalculator")
    
    def calculate_weights(
        self,
        catchments: List[Dict[str, Any]],
        pixel_mappings: Dict[int, List[int]],
        output_dir: Optional[Path] = None,
        force_simple: bool = False
    ) -> Dict[Tuple[int, int], float]:
        """
        Calculate pixel area weights.
        
        Args:
            catchments: List of catchment dictionaries with geometries
            pixel_mappings: Dict mapping catchment_id -> list of pixel indices
            output_dir: Directory to save weights JSON (optional)
            force_simple: Force simple method (skip geometric)
            
        Returns:
            Dict mapping (catchment_id, pixel_index) -> weight (0.0 to 1.0)
            
        Example:
            >>> weights = calculator.calculate_weights(
            ...     catchments=[{...}],
            ...     pixel_mappings={1: [10, 11], 2: [11, 12]},
            ...     output_dir=Path("outputs/rain_radar/20250509-20250510/raw")
            ... )
            >>> # If pixel 11 shared by catchments 1 and 2:
            >>> weights[(1, 11)]  # e.g., 0.6 (60% in catchment 1)
            >>> weights[(2, 11)]  # e.g., 0.4 (40% in catchment 2)
        """
        self._logger.info("Calculating pixel area weights for de-duplication...")
        
        # Try geometric weights first (unless forced simple)
        if not force_simple:
            try:
                self._logger.info("  Strategy: Geometric intersection (API pixel metadata)")
                weights = self._calculate_geometric_weights(
                    catchments=catchments,
                    pixel_mappings=pixel_mappings
                )
                self._logger.info("  ✓ Geometric weights calculated successfully")
            except Exception as e:
                self._logger.warning(
                    f"  Geometric weighting failed: {e}\n"
                    f"  Falling back to simple equal-split method"
                )
                weights = self._calculate_simple_weights(
                    catchments=catchments,
                    pixel_mappings=pixel_mappings
                )
        else:
            # Simple method only
            self._logger.info("  Strategy: Simple equal-split (forced)")
            weights = self._calculate_simple_weights(
                catchments=catchments,
                pixel_mappings=pixel_mappings
            )
        
        # Save weights to JSON if output directory provided
        if output_dir:
            self._save_weights(weights, output_dir)
        
        # Log statistics
        self._log_weight_statistics(weights)
        
        return weights
    
    def _calculate_geometric_weights(
        self,
        catchments: List[Dict[str, Any]],
        pixel_mappings: Dict[int, List[int]]
    ) -> Dict[Tuple[int, int], float]:
        """
        Calculate weights using geometric intersection areas.
        
        Args:
            catchments: Catchment geometries
            pixel_mappings: Pixel mappings per catchment
            
        Returns:
            Weight dictionary
            
        Raises:
            Exception: If API calls fail or geometries invalid
        """
        return calculate_geometric_pixel_weights_from_api(
            catchments=catchments,
            pixel_mappings=pixel_mappings,
            client=self._client,
            collection_id=self._collection_id
        )
    
    def _calculate_simple_weights(
        self,
        catchments: List[Dict[str, Any]],
        pixel_mappings: Dict[int, List[int]]
    ) -> Dict[Tuple[int, int], float]:
        """
        Calculate weights using simple equal-split method.
        
        If a pixel is shared by N catchments, each gets weight 1/N.
        
        Args:
            catchments: Catchment geometries
            pixel_mappings: Pixel mappings per catchment
            
        Returns:
            Weight dictionary
        """
        return estimate_pixel_area_weights_simple(
            catchments=catchments,
            pixel_mappings=pixel_mappings
        )
    
    def _save_weights(
        self,
        weights: Dict[Tuple[int, int], float],
        output_dir: Path
    ) -> None:
        """
        Save weights to JSON file.
        
        Args:
            weights: Weight dictionary
            output_dir: Directory to save to
        """
        weights_file = output_dir / "pixel_weights.json"
        
        try:
            save_pixel_weights(weights, weights_file)
            self._logger.info(f"  ✓ Weights saved to: {weights_file}")
        except Exception as e:
            self._logger.warning(f"  Failed to save weights: {e}")
    
    def _log_weight_statistics(
        self,
        weights: Dict[Tuple[int, int], float]
    ) -> None:
        """
        Log statistics about calculated weights.
        
        Args:
            weights: Weight dictionary
        """
        if not weights:
            self._logger.warning("  No weights calculated")
            return
        
        # Count weighted pixels (weight < 1.0 = shared by multiple catchments)
        weighted_pixels = sum(1 for w in weights.values() if w < 1.0)
        total_pairs = len(weights)
        
        if total_pairs > 0:
            percentage = 100 * weighted_pixels / total_pairs
            self._logger.info(
                f"  ✓ Weighted {weighted_pixels}/{total_pairs} "
                f"pixel-catchment pairs ({percentage:.1f}%)"
            )
        
        # Find maximum overlap (minimum weight)
        if weights:
            min_weight = min(weights.values())
            max_overlap = int(1.0 / min_weight) if min_weight > 0 else 0
            if max_overlap > 1:
                self._logger.debug(
                    f"  Maximum overlap: {max_overlap} catchments share a pixel "
                    f"(weight={min_weight:.4f})"
                )
