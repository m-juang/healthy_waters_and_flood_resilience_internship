"""Client for asset-related API operations."""
from typing import Dict, List, Any, Optional, Union

from .base import BaseClient
from .. import endpoints as ep


class AssetClient(BaseClient):
    """
    Client for asset operations (rain gauges, catchments, etc.).
    
    This client handles all asset-related API calls including:
    - Fetching assets by project
    - Getting rain gauge assets
    - Getting catchment assets
    - Fetching asset geometry
    - Getting single asset by ID
    
    Single Responsibility: Asset management only
    
    Example:
        >>> from moata_pipeline.moata.clients import AssetClient
        >>> from moata_pipeline.moata.http import MoataHttp
        >>> 
        >>> client = AssetClient(http=http_client)
        >>> gauges = client.get_rain_gauges(project_id=594, asset_type_id=100)
    """
    
    def get_asset_by_id(self, asset_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """
        Get a single asset by ID.
        
        Uses GET /v1/assets/{assetId} endpoint.
        
        Args:
            asset_id: The asset ID to fetch
            
        Returns:
            Asset dictionary with id, name, description, assetType, projectId, etc.
            Returns None if asset not found (404).
            
        Example:
            >>> asset = client.get_asset_by_id(3880960)
            >>> print(asset["name"])  # "ACC - Rain - Takapuna Rain @ Library"
        """
        aid = self._validate_id(asset_id, "asset_id")
        
        url = ep.ASSET_DETAIL.format(asset_id=aid)
        self._log_request("GET", url)
        
        try:
            response = self._http.get(url)
            self._logger.debug(f"Asset {aid}: {response.get('name', 'unnamed')}")
            return response
        except Exception as e:
            # Handle 404 gracefully
            if "404" in str(e) or "not found" in str(e).lower():
                self._logger.warning(f"Asset {aid} not found")
                return None
            raise
    
    def get_rain_gauges(
        self,
        project_id: Union[int, str],
        asset_type_id: Union[int, str],
        exclude_keyword: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get rain gauge assets for a project.
        
        Args:
            project_id: Moata project ID
            asset_type_id: Rain gauge asset type ID (typically 25)
            exclude_keyword: Optional keyword to exclude assets (case-insensitive)
            
        Returns:
            List of rain gauge asset dictionaries
            
        Raises:
            ValidationError: If parameters are invalid
            
        Example:
            >>> gauges = client.get_rain_gauges(594, 25, exclude_keyword="test")
            >>> print(f"Found {len(gauges)} rain gauges")
        """
        # Validate parameters
        pid = self._validate_id(project_id, "project_id")
        atid = self._validate_id(asset_type_id, "asset_type_id")
        
        # Build URL
        url = ep.PROJECT_ASSETS.format(project_id=pid)
        params = {"assetTypeId": atid}
        self._log_request("GET", url)
        
        # Make request
        response = self._http.get(url, params=params)
        assets = self._extract_items(response)
        
        # Filter by exclude keyword if provided
        if exclude_keyword:
            keyword_lower = exclude_keyword.lower()
            assets = [
                a for a in assets
                if keyword_lower not in a.get("name", "").lower()
            ]
            self._logger.debug(
                f"Filtered {len(assets)} assets (excluding '{exclude_keyword}')"
            )
        
        self._logger.info(f"Retrieved {len(assets)} rain gauge assets")
        return assets
    
    def get_assets_with_geometry(
        self,
        project_id: Union[int, str],
        asset_type_id: Union[int, str],
        exclude_keyword: Optional[str] = None,
        sr_id: int = 4326
    ) -> List[Dict[str, Any]]:
        """
        Get assets with detailed geometry information.
        
        This fetches assets with full GeoJSON geometry, useful for
        spatial operations like catchment analysis.
        
        Args:
            project_id: Moata project ID
            asset_type_id: Asset type ID
            exclude_keyword: Optional keyword to exclude assets
            sr_id: Spatial reference system ID (default: 4326 for WGS84)
            
        Returns:
            List of asset dictionaries with geometry
            
        Example:
            >>> catchments = client.get_assets_with_geometry(
            ...     project_id=594,
            ...     asset_type_id=3541,  # Stormwater catchments
            ...     sr_id=4326
            ... )
        """
        # Validate parameters
        pid = self._validate_id(project_id, "project_id")
        atid = self._validate_id(asset_type_id, "asset_type_id")
        
        # Build URL
        url = ep.PROJECT_ASSETS.format(project_id=pid)
        params = {"assetTypeId": atid, "srId": sr_id}
        self._log_request("GET", url)
        
        # Make request
        response = self._http.get(url, params=params)
        assets = self._extract_items(response)
        
        # Filter by exclude keyword if provided
        if exclude_keyword:
            keyword_lower = exclude_keyword.lower()
            assets = [
                a for a in assets
                if keyword_lower not in a.get("name", "").lower()
            ]
        
        self._logger.info(
            f"Retrieved {len(assets)} assets with geometry (SR: {sr_id})"
        )
        return assets
    
    def get_asset_names_batch(
        self,
        asset_ids: List[Union[int, str]],
        max_per_request: int = 50
    ) -> Dict[int, str]:
        """
        Get names for multiple assets efficiently.
        
        Makes individual GET /v1/assets/{assetId} calls for each asset.
        Useful when asset names are not available from project-level endpoints.
        
        Args:
            asset_ids: List of asset IDs to look up
            max_per_request: Maximum concurrent requests (for rate limiting)
            
        Returns:
            Dictionary mapping asset_id -> asset_name
            Assets not found will have value "Asset {id}"
            
        Example:
            >>> names = client.get_asset_names_batch([3880960, 3880961])
            >>> print(names[3880960])  # "ACC - Rain - Takapuna Rain @ Library"
        """
        result: Dict[int, str] = {}
        
        for asset_id in asset_ids:
            try:
                aid = self._validate_id(asset_id, "asset_id")
                asset = self.get_asset_by_id(aid)
                if asset and asset.get("name"):
                    result[aid] = asset["name"]
                else:
                    result[aid] = f"Asset {aid}"
            except Exception as e:
                self._logger.warning(f"Failed to get name for asset {asset_id}: {e}")
                try:
                    aid = int(asset_id)
                    result[aid] = f"Asset {aid}"
                except (ValueError, TypeError):
                    pass
        
        self._logger.info(f"Retrieved names for {len(result)} assets")
        return result
