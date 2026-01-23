"""Asset fetching and preparation logic."""
from typing import Any, Dict, List, Tuple

from .base import BaseCollector, HTTPClientProtocol
from moata_pipeline.common.typing_utils import safe_int


class AssetFetcher(BaseCollector):
    """
    Fetches and prepares asset data.
    
    Single Responsibility: Asset operations only
    
    Responsibilities:
    - Fetch rain gauge assets from API
    - Extract asset IDs
    - Create asset lookup dictionaries
    
    Example:
        >>> fetcher = AssetFetcher(client)
        >>> gauges = fetcher.fetch_gauges(594, 25)
        >>> asset_ids, gauge_by_id = fetcher.prepare_asset_lookup(gauges)
    """
    
    def fetch_gauges(
        self,
        project_id: int,
        asset_type_id: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch rain gauge assets.
        
        Args:
            project_id: Moata project ID
            asset_type_id: Asset type ID for rain gauges
            
        Returns:
            List of gauge asset dictionaries
            
        Raises:
            ValueError: If parameters are invalid
            
        Example:
            >>> gauges = fetcher.fetch_gauges(594, 25)
            >>> print(f"Found {len(gauges)} gauges")
        """
        self._validate_positive_int(project_id, "project_id")
        self._validate_positive_int(asset_type_id, "asset_type_id")
        
        self._logger.info("Fetching rain gauges...")
        self._logger.info(f"  Project ID: {project_id}")
        self._logger.info(f"  Asset Type ID: {asset_type_id}")
        
        gauges = self._client.get_rain_gauges(
            project_id=project_id,
            asset_type_id=asset_type_id
        )
        
        self._logger.info(f"✓ Fetched {len(gauges)} rain gauges")
        return gauges
    
    def prepare_asset_lookup(
        self,
        gauges: List[Dict[str, Any]]
    ) -> Tuple[List[int], Dict[int, Dict[str, Any]]]:
        """
        Extract asset IDs and create lookup dictionary.
        
        Args:
            gauges: List of gauge asset dictionaries
            
        Returns:
            Tuple of (asset_ids, gauge_by_id)
            - asset_ids: List of integer asset IDs
            - gauge_by_id: Dictionary mapping asset_id -> gauge data
            
        Example:
            >>> asset_ids, gauge_by_id = fetcher.prepare_asset_lookup(gauges)
            >>> gauge = gauge_by_id[12345]
        """
        self._logger.info("Preparing asset lookup...")
        
        asset_ids: List[int] = []
        gauge_by_id: Dict[int, Dict[str, Any]] = {}
        
        for gauge in gauges:
            asset_id = safe_int(gauge.get("id"))
            if asset_id is None:
                self._logger.warning(f"Skipping gauge with invalid ID: {gauge.get('id')}")
                continue
            
            asset_ids.append(asset_id)
            gauge_by_id[asset_id] = gauge
        
        self._logger.info(f"✓ Prepared {len(asset_ids)} valid asset IDs")
        
        if len(asset_ids) != len(gauges):
            skipped = len(gauges) - len(asset_ids)
            self._logger.warning(f"Skipped {skipped} gauges with invalid IDs")
        
        return asset_ids, gauge_by_id
