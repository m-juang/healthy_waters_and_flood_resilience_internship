"""
Catchment Fetcher Module

Fetches stormwater catchment geometries from Moata API.

Classes:
    CatchmentFetcher: Fetches and processes catchment assets

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 1.0.0 - Extracted from RadarDataCollector
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from moata_pipeline.moata.client import MoataClient


class CatchmentFetcher:
    """
    Fetches stormwater catchment geometries from Moata API.
    
    Responsible for:
    - Fetching catchment assets with geometries
    - Validating catchment data
    - Organizing catchment metadata
    
    Args:
        client: Authenticated MoataClient instance
        
    Example:
        >>> fetcher = CatchmentFetcher(client)
        >>> catchments = fetcher.fetch_catchments(project_id=594, asset_type_id=3541)
        >>> print(f"Fetched {len(catchments)} catchments")
    """
    
    def __init__(self, client: MoataClient) -> None:
        """
        Initialize catchment fetcher.
        
        Args:
            client: Authenticated MoataClient instance
        """
        self._client = client
        self._logger = logging.getLogger(f"{__name__}.CatchmentFetcher")
    
    def fetch_catchments(
        self,
        project_id: int,
        asset_type_id: int = 3541,
        sr_id: int = 4326,
    ) -> List[Dict[str, Any]]:
        """
        Fetch stormwater catchments with geometries.
        
        Args:
            project_id: Moata project ID
            asset_type_id: Asset type ID for stormwater catchments (default: 3541)
            sr_id: Spatial reference ID (default: 4326 = WGS84)
            
        Returns:
            List of catchment dictionaries with geometries
            
        Example:
            >>> catchments = fetcher.fetch_catchments(project_id=594)
            >>> for c in catchments:
            ...     print(f"{c['name']}: {c['id']}")
        """
        self._logger.info(
            f"Fetching stormwater catchments "
            f"(project={project_id}, assetType={asset_type_id}, srId={sr_id})..."
        )
        
        # Fetch catchments with geometries
        catchments = self._client.get_assets_with_geometry(
            project_id=project_id,
            asset_type_id=asset_type_id,
            sr_id=sr_id
        )
        
        self._logger.info(f"Retrieved {len(catchments)} assets with geometry (SR: {sr_id})")
        
        # Normalize and validate catchments
        valid_catchments = []
        for catchment in catchments:
            normalized = self._normalize_catchment(catchment)
            if not self._validate_catchment(normalized):
                continue
            valid_catchments.append(normalized)
        
        self._logger.info(f"✓ Fetched {len(valid_catchments)} stormwater catchments")
        return valid_catchments
    
    def _normalize_catchment(self, catchment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize API response field names to internal format.
        
        API returns fields like 'assetName', 'assetId', 'geometryWkt'
        but internal code expects 'name', 'id', 'geometry'.
        
        Args:
            catchment: Raw catchment dictionary from API
            
        Returns:
            Normalized catchment dictionary with consistent field names
        """
        normalized = dict(catchment)
        
        # Normalize name field (API uses 'assetName', we use 'name')
        if "name" not in normalized and "assetName" in normalized:
            normalized["name"] = normalized["assetName"]
        
        # Normalize id field (API might use 'assetId', we use 'id')
        if "id" not in normalized and "assetId" in normalized:
            normalized["id"] = normalized["assetId"]
        
        # Normalize geometry field (API uses 'geometryWkt', we use 'geometry')
        if "geometry" not in normalized and "geometryWkt" in normalized:
            normalized["geometry"] = normalized["geometryWkt"]
        
        return normalized
        
        self._logger.info(f"✓ Fetched {len(valid_catchments)} stormwater catchments")
        return valid_catchments
    
    def _validate_catchment(self, catchment: Dict[str, Any]) -> bool:
        """
        Validate catchment has required fields.
        
        Args:
            catchment: Catchment dictionary (should be normalized first)
            
        Returns:
            True if valid, False otherwise
        """
        # Get name for logging (check both normalized and raw field names)
        catchment_name = catchment.get('name') or catchment.get('assetName') or 'Unknown'
        
        required_fields = ["id", "name", "geometry"]
        
        for field in required_fields:
            if field not in catchment:
                self._logger.warning(
                    f"Catchment missing {field}: {catchment_name}"
                )
                return False
        
        # Check geometry is not None/empty
        geometry = catchment.get("geometry")
        if not geometry:
            self._logger.warning(
                f"Catchment has empty geometry: {catchment_name}"
            )
            return False
        
        return True
    
    def prepare_catchment_lookup(
        self,
        catchments: List[Dict[str, Any]]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Create lookup dictionary for catchments by ID.
        
        Args:
            catchments: List of catchment dictionaries
            
        Returns:
            Dictionary mapping catchment_id -> catchment data
            
        Example:
            >>> lookup = fetcher.prepare_catchment_lookup(catchments)
            >>> catchment = lookup[123]
        """
        catchment_by_id = {}
        
        for catchment in catchments:
            catchment_id = catchment.get("id")
            if catchment_id is not None:
                catchment_by_id[int(catchment_id)] = catchment
        
        self._logger.debug(f"Created lookup for {len(catchment_by_id)} catchments")
        return catchment_by_id
