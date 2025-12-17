"""
Rain Radar (QPE) Client - extends MoataClient with radar-specific methods
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
from .http import MoataHttp
from . import radar_endpoints as rep
from . import endpoints as ep


class RadarClient:
    """
    Client for Rain Radar (QPE - Quantitative Precipitation Estimation) data
    
    Based on Sam's email instructions for accessing radar data:
    - TraceSet Collection ID: 1
    - TraceSet ID: 3
    - Asset Type ID: 3541 (Stormwater catchments)
    """
    
    def __init__(self, http: MoataHttp) -> None:
        self._http = http
    
    def get_stormwater_catchments(
        self, 
        project_id: int,
        srid: int = 4326,
        asset_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all stormwater catchments with geometries
        
        Args:
            project_id: Project ID (594 for Auckland Council)
            srid: Spatial Reference ID (4326 for WGS84)
            asset_name: Optional filter by name
            
        Returns:
            List of catchment assets with geometries
        """
        path = ep.PROJECT_ASSETS.format(project_id=project_id)
        params = {
            "assetTypeId": rep.STORMWATER_ASSET_TYPE_ID,
            "srId": srid
        }
        if asset_name:
            params["assetName"] = asset_name
        
        data = self._http.get(path, params=params)
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []
    
    def get_pixel_mappings_for_geometry(
        self,
        geometry_wkt: str,
        srid: int = 4326,
        collection_id: int = rep.RADAR_TRACESET_COLLECTION_ID
    ) -> List[Dict[str, Any]]:
        """
        Get pixel indexes that intersect with a geometry
        
        Args:
            geometry_wkt: Well-Known Text representation of geometry
            srid: Spatial Reference ID
            collection_id: TraceSet Collection ID (1 for radar)
            
        Returns:
            List of pixel mappings with pixelIndex and geometryWkt
        """
        path = rep.TRACESET_PIXEL_MAPPINGS.format(collection_id=collection_id)
        params = {
            "srId": srid,
            "wkt": geometry_wkt
        }
        
        data = self._http.get(path, params=params)
        return data if isinstance(data, list) else []
    
    def get_radar_data(
        self,
        pixel_indexes: List[int],
        start_time: str,
        end_time: str,
        traceset_ids: Optional[List[int]] = None,
        collection_id: int = rep.RADAR_TRACESET_COLLECTION_ID
    ) -> List[Dict[str, Any]]:
        """
        Get radar data for specified pixels and time range
        
        IMPORTANT LIMITATIONS (from Sam):
        - Max 150 pixels per request (recommend 50)
        - Max ~24 hours of data per request (minute resolution)
        - Data is live and can be revised
        
        Args:
            pixel_indexes: List of pixel indexes (max 150, recommend 50)
            start_time: ISO format UTC string (e.g., "2024-01-15T00:00:00Z")
            end_time: ISO format UTC string
            traceset_ids: List of traceset IDs (default [3] for radar)
            collection_id: TraceSet Collection ID (1 for radar)
            
        Returns:
            List of radar data records with values per pixel
        """
        if traceset_ids is None:
            traceset_ids = [rep.RADAR_TRACESET_ID]
        
        # Validate pixel count
        if len(pixel_indexes) > 150:
            raise ValueError(
                f"Too many pixels ({len(pixel_indexes)}). Max 150 per request. "
                "Consider batching with batch_get_radar_data() instead."
            )
        
        path = rep.TRACESET_COLLECTION_DATA.format(collection_id=collection_id)
        params = {
            "TsId": traceset_ids,
            "StartTime": start_time,
            "EndTime": end_time,
            "Pi": pixel_indexes
        }
        
        data = self._http.get(path, params=params)
        return data if isinstance(data, list) else []
    
    def batch_get_radar_data(
        self,
        pixel_indexes: List[int],
        start_time: str,
        end_time: str,
        batch_size: int = 50,
        traceset_ids: Optional[List[int]] = None,
        collection_id: int = rep.RADAR_TRACESET_COLLECTION_ID
    ) -> List[Dict[str, Any]]:
        """
        Get radar data with automatic batching for large pixel lists
        
        Args:
            pixel_indexes: List of pixel indexes (any size)
            start_time: ISO format UTC string
            end_time: ISO format UTC string
            batch_size: Pixels per batch (default 50, max 150)
            traceset_ids: List of traceset IDs (default [3])
            collection_id: TraceSet Collection ID (1 for radar)
            
        Returns:
            Combined list of all radar data records
        """
        all_data = []
        
        for i in range(0, len(pixel_indexes), batch_size):
            batch_pixels = pixel_indexes[i:i+batch_size]
            
            batch_data = self.get_radar_data(
                pixel_indexes=batch_pixels,
                start_time=start_time,
                end_time=end_time,
                traceset_ids=traceset_ids,
                collection_id=collection_id
            )
            
            all_data.extend(batch_data)
        
        return all_data