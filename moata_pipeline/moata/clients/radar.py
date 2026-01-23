"""Client for radar-related API operations."""
from typing import Dict, List, Any, Union, Optional
import logging

from .base import BaseClient
from .. import endpoints as ep

# Constants
DEFAULT_RADAR_BATCH_SIZE = 50
MAX_RADAR_BATCH_SIZE = 150


class RadarClient(BaseClient):
    """
    Client for radar operations (QPE, TraceSet, pixel mappings).
    
    This client handles:
    - Pixel mappings for catchments
    - TraceSet data (radar rainfall)
    - Batched TraceSet requests
    - Pixel mapping metadata
    
    Single Responsibility: Radar/QPE data management
    
    Example:
        >>> client = RadarClient(http=http_client)
        >>> pixels = client.get_pixel_mappings_for_geometry(
        ...     traceset_id=123,
        ...     geometry=catchment_geojson
        ... )
        >>> data = client.get_traceset_data(
        ...     traceset_id=123,
        ...     pixel_ids=[1, 2, 3],
        ...     from_time="2025-01-01T00:00:00Z",
        ...     to_time="2025-01-31T23:59:59Z"
        ... )
    """
    
    def get_pixel_mappings_for_geometry(
        self,
        traceset_id: Union[int, str],
        geometry: Union[Dict[str, Any], str],
        sr_id: int = 4326
    ) -> List[Dict[str, Any]]:
        """
        Get pixel mappings that intersect with a geometry.
        
        This finds which radar pixels overlap with a catchment
        or other geometry, essential for calculating catchment rainfall.
        
        Args:
            traceset_id: Radar TraceSet collection ID
            geometry: Either GeoJSON geometry dict or WKT string
            sr_id: Spatial reference ID (default: 4326 for WGS84)
            
        Returns:
            List of pixel mapping dictionaries with pixel IDs and weights
            
        Example:
            >>> # Using GeoJSON
            >>> pixels = client.get_pixel_mappings_for_geometry(
            ...     traceset_id=1,
            ...     geometry={
            ...         "type": "Polygon",
            ...         "coordinates": [[[174.7, -36.8], ...]]
            ...     }
            ... )
            >>> # Or using WKT
            >>> pixels = client.get_pixel_mappings_for_geometry(
            ...     traceset_id=1,
            ...     geometry="POLYGON((174.7 -36.8, ...))"
            ... )
        """
        tid = self._validate_id(traceset_id, "traceset_id")
        
        # Handle both GeoJSON and WKT formats
        if isinstance(geometry, str):
            # WKT string
            wkt = geometry
        elif isinstance(geometry, dict):
            # GeoJSON - convert to WKT (basic conversion)
            # For production, use shapely: from shapely.geometry import shape; shape(geometry).wkt
            from .base import ValidationError
            raise ValidationError(
                "GeoJSON geometry support requires shapely library. "
                "Please pass WKT string directly or install shapely."
            )
        else:
            from .base import ValidationError
            raise ValidationError("geometry must be a dict (GeoJSON) or string (WKT)")
        
        path = ep.TRACESET_PIXEL_MAPPINGS.format(collection_id=tid)
        params = {"wkt": wkt, "srId": sr_id}
        self._log_request("GET", path)
        
        # Make request
        data = self._http.get(path, params=params, allow_404=True)
        
        if data is None:
            self._logger.debug(f"No pixel mappings found for TraceSet {tid}")
            return []
        
        pixels = self._extract_items(data)
        
        self._logger.info(
            f"Retrieved {len(pixels)} pixel mappings for TraceSet {tid}"
        )
        return pixels
    
    def get_traceset_data(
        self,
        traceset_id: Union[int, str],
        pixel_ids: List[int],
        from_time: str,
        to_time: str
    ) -> Dict[str, Any]:
        """
        Get radar data for specific pixels.
        
        Fetches rainfall values for radar pixels over a time range.
        
        Args:
            traceset_id: Radar TraceSet collection ID (e.g., 1 for QPE)
            pixel_ids: List of pixel indices to fetch (max 150)
            from_time: Start time (ISO 8601)
            to_time: End time (ISO 8601)
            
        Returns:
            List of pixel data dictionaries
            
        Example:
            >>> data = client.get_traceset_data(
            ...     traceset_id=1,
            ...     pixel_ids=[100, 101, 102],
            ...     from_time="2025-01-01T00:00:00Z",
            ...     to_time="2025-01-31T23:59:59Z"
            ... )
        """
        tid = self._validate_id(traceset_id, "traceset_id")
        self._validate_time_string(from_time, "from_time")
        self._validate_time_string(to_time, "to_time")
        
        if not pixel_ids:
            self._logger.warning("pixel_ids is empty, returning empty result")
            return []
        
        from .base import ValidationError
        if len(pixel_ids) > MAX_RADAR_BATCH_SIZE:
            raise ValidationError(
                f"pixel_ids exceeds maximum of {MAX_RADAR_BATCH_SIZE}. "
                f"Use get_traceset_data_batched() for larger requests."
            )
        
        path = ep.TRACESET_COLLECTION_DATA.format(collection_id=tid)
        params = {
            "TsId": [3],  # TraceSet ID for QPE (typically 3)
            "Pi": [int(x) for x in pixel_ids],
            "StartTime": from_time,
            "EndTime": to_time
        }
        self._log_request("GET", path)
        
        # Make request
        data = self._http.get(path, params=params, allow_404=True)
        
        if data is None:
            self._logger.debug(f"No data returned for TraceSet {tid}")
            return []
        
        items = self._extract_items(data)
        
        # Log summary
        self._logger.info(
            f"Retrieved data for {len(items)} pixels (TraceSet {tid})"
        )
        
        return items
    
    def get_traceset_data_batched(
        self,
        traceset_id: Union[int, str],
        pixel_ids: List[int],
        from_time: str,
        to_time: str,
        batch_size: int = DEFAULT_RADAR_BATCH_SIZE
    ) -> Dict[str, Any]:
        """
        Get radar data for pixels in batches.
        
        When fetching data for many pixels, this splits the request
        into smaller batches to avoid timeouts and API limits.
        
        Args:
            traceset_id: Radar TraceSet ID
            pixel_ids: List of pixel IDs to fetch
            from_time: Start time (ISO 8601)
            to_time: End time (ISO 8601)
            batch_size: Pixels per batch (default: 50, max: 150)
            
        Returns:
            Combined dictionary with all pixel data
            
        Example:
            >>> # Fetch data for 200 pixels in batches
            >>> data = client.get_traceset_data_batched(
            ...     traceset_id=123,
            ...     pixel_ids=list(range(1, 201)),
            ...     from_time="2025-01-01T00:00:00Z",
            ...     to_time="2025-01-31T23:59:59Z",
            ...     batch_size=50
            ... )
        """
        from .base import ValidationError
        
        if batch_size > MAX_RADAR_BATCH_SIZE:
            self._logger.warning(
                f"batch_size {batch_size} exceeds max {MAX_RADAR_BATCH_SIZE}, "
                f"using {MAX_RADAR_BATCH_SIZE}"
            )
            batch_size = MAX_RADAR_BATCH_SIZE
        
        if not pixel_ids:
            return {"times": [], "pixels": {}}
        
        # If small enough, fetch in one go
        if len(pixel_ids) <= batch_size:
            return self.get_traceset_data(traceset_id, pixel_ids, from_time, to_time)
        
        # Split into batches
        batches = [
            pixel_ids[i:i + batch_size]
            for i in range(0, len(pixel_ids), batch_size)
        ]
        
        self._logger.info(
            f"Fetching {len(pixel_ids)} pixels in {len(batches)} batches "
            f"(batch_size={batch_size})"
        )
        
        # Fetch each batch
        combined_results = []
        
        for i, batch in enumerate(batches, 1):
            self._logger.debug(f"Fetching batch {i}/{len(batches)} ({len(batch)} pixels)")
            
            batch_data = self.get_traceset_data(
                traceset_id, batch, from_time, to_time
            )
            
            # Collect results
            if isinstance(batch_data, list):
                combined_results.extend(batch_data)
        
        self._logger.info(
            f"Combined {len(combined_results)} pixel results from {len(batches)} batches"
        )
        
        return combined_results
    
    def get_pixel_mappings_metadata(
        self,
        traceset_id: Union[int, str],
        pixel_ids: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get metadata for pixel mappings.
        
        Fetches detailed information about radar pixels including
        geometry, spatial info, etc.
        
        Args:
            traceset_id: Radar TraceSet ID
            pixel_ids: Optional list of specific pixel IDs
            
        Returns:
            List of pixel metadata dictionaries
            
        Example:
            >>> metadata = client.get_pixel_mappings_metadata(
            ...     traceset_id=123,
            ...     pixel_ids=[1, 2, 3]
            ... )
        """
        tid = self._validate_id(traceset_id, "traceset_id")
        
        path = ep.TRACESET_PIXEL_METADATA.format(collection_id=tid)
        
        if pixel_ids:
            # GET with pixel IDs as query params
            params = {"pixelIndices": pixel_ids}
            self._log_request("GET", path)
            response = self._http.get(path, params=params, allow_404=True)
        else:
            # GET all metadata
            self._log_request("GET", path)
            response = self._http.get(path, allow_404=True)
        
        if response is None:
            self._logger.debug(f"No metadata found for TraceSet {tid}")
            return []
        
        metadata = self._extract_items(response)
        
        self._logger.info(
            f"Retrieved metadata for {len(metadata)} pixels (TraceSet {tid})"
        )
        return metadata
