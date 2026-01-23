"""
Radar Data Fetcher Module

Fetches radar QPE timeseries data from Moata API.

Classes:
    RadarDataFetcher: Fetches radar timeseries for pixels

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 1.0.0 - Extracted from RadarDataCollector
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from moata_pipeline.moata.client import MoataClient
from moata_pipeline.common.iter_utils import chunk
from moata_pipeline.common.time_utils import iso_z


class RadarDataFetcher:
    """
    Fetches radar QPE (Quantitative Precipitation Estimation) timeseries data.
    
    Responsible for:
    - Fetching radar data for pixel lists
    - Batching requests to stay within API limits
    - Combining results from multiple batches
    - Converting to DataFrame format
    
    Args:
        client: Authenticated MoataClient instance
        pixel_batch_size: Number of pixels per API request (default: 50, max: 150)
        
    Example:
        >>> fetcher = RadarDataFetcher(client, pixel_batch_size=50)
        >>> df = fetcher.fetch_radar_data(
        ...     pixels=[1, 2, 3],
        ...     start_time=start,
        ...     end_time=end,
        ...     collection_id=1,
        ...     traceset_id=3
        ... )
    """
    
    DEFAULT_COLLECTION_ID = 1  # Sam's recommended collection ID for QPE
    DEFAULT_TRACESET_ID = 3    # Sam's recommended traceset ID for QPE
    
    def __init__(
        self,
        client: MoataClient,
        pixel_batch_size: int = 50
    ) -> None:
        """
        Initialize radar data fetcher.
        
        Args:
            client: Authenticated MoataClient instance
            pixel_batch_size: Pixels per API request (1-150, default: 50)
            
        Raises:
            ValueError: If pixel_batch_size out of range
        """
        if not 1 <= pixel_batch_size <= 150:
            raise ValueError(
                f"pixel_batch_size must be 1-150, got {pixel_batch_size}"
            )
        
        self._client = client
        self._pixel_batch_size = pixel_batch_size
        self._logger = logging.getLogger(f"{__name__}.RadarDataFetcher")
    
    def fetch_radar_data(
        self,
        pixels: List[int],
        start_time: datetime,
        end_time: datetime,
        collection_id: int = DEFAULT_COLLECTION_ID,
        traceset_id: int = DEFAULT_TRACESET_ID,
    ) -> pd.DataFrame:
        """
        Fetch radar data for multiple pixels.
        
        Args:
            pixels: List of pixel indices
            start_time: Start of time range (UTC)
            end_time: End of time range (UTC)
            collection_id: Radar collection ID (default: 1)
            traceset_id: Traceset ID (default: 3)
            
        Returns:
            DataFrame with columns: ['pixel_index', 'timestamp', 'value']
            
        Example:
            >>> df = fetcher.fetch_radar_data(
            ...     pixels=[1, 2, 3],
            ...     start_time=datetime(2025, 5, 9, 0, 0, tzinfo=timezone.utc),
            ...     end_time=datetime(2025, 5, 10, 0, 0, tzinfo=timezone.utc)
            ... )
            >>> print(f"Fetched {len(df)} records")
        """
        if not pixels:
            self._logger.warning("No pixels to fetch data for")
            return pd.DataFrame(columns=["pixel_index", "timestamp", "value"])
        
        self._logger.info(
            f"  Fetching radar data: {len(pixels)} pixels, "
            f"{iso_z(start_time)} to {iso_z(end_time)}"
        )
        
        # Batch pixels to stay within API limits
        if len(pixels) <= self._pixel_batch_size:
            # Single request
            return self._fetch_single_batch(
                pixels=pixels,
                start_time=start_time,
                end_time=end_time,
                collection_id=collection_id,
                traceset_id=traceset_id
            )
        
        # Multiple batches
        return self._fetch_multiple_batches(
            pixels=pixels,
            start_time=start_time,
            end_time=end_time,
            collection_id=collection_id,
            traceset_id=traceset_id
        )
    
    def _fetch_single_batch(
        self,
        pixels: List[int],
        start_time: datetime,
        end_time: datetime,
        collection_id: int,
        traceset_id: int,
    ) -> pd.DataFrame:
        """
        Fetch radar data for single batch of pixels.
        
        Args:
            pixels: List of pixel indices
            start_time: Start time (UTC)
            end_time: End time (UTC)
            collection_id: Collection ID
            traceset_id: Traceset ID
            
        Returns:
            DataFrame with radar data
        """
        try:
            # Call API - use correct method name: get_traceset_data
            # Parameters: traceset_id, pixel_ids (list), from_time (ISO string), to_time (ISO string)
            data = self._client.get_traceset_data(
                traceset_id=collection_id,  # collection_id is used as traceset_id
                pixel_ids=pixels,
                from_time=iso_z(start_time),
                to_time=iso_z(end_time)
            )
            
            # Convert to DataFrame
            df = self._parse_radar_response(data)
            self._logger.info(f"  ✓ Fetched data for {len(df)} pixel-traceset combinations")
            return df
            
        except Exception as e:
            self._logger.error(f"Failed to fetch radar data: {e}")
            return pd.DataFrame(columns=["pixel_index", "timestamp", "value"])
    
    def _fetch_multiple_batches(
        self,
        pixels: List[int],
        start_time: datetime,
        end_time: datetime,
        collection_id: int,
        traceset_id: int,
    ) -> pd.DataFrame:
        """
        Fetch radar data across multiple batches.
        
        Args:
            pixels: List of pixel indices
            start_time: Start time (UTC)
            end_time: End time (UTC)
            collection_id: Collection ID
            traceset_id: Traceset ID
            
        Returns:
            Combined DataFrame with all radar data
        """
        batches = list(chunk(pixels, self._pixel_batch_size))
        num_batches = len(batches)
        
        self._logger.info(f"Fetching {len(pixels)} pixels in {num_batches} batches (batch_size={self._pixel_batch_size})")
        
        all_dfs = []
        
        for batch_idx, pixel_batch in enumerate(batches, start=1):
            self._logger.debug(f"  Batch {batch_idx}/{num_batches}: {len(pixel_batch)} pixels")
            
            try:
                # Fetch this batch
                data = self._client.get_traceset_data(
                    traceset_id=collection_id,
                    pixel_ids=pixel_batch,
                    from_time=iso_z(start_time),
                    to_time=iso_z(end_time)
                )
                
                # Parse to DataFrame
                df = self._parse_radar_response(data)
                if not df.empty:
                    all_dfs.append(df)
                
            except Exception as e:
                self._logger.error(f"Batch {batch_idx} failed: {e}")
                continue
        
        # Combine all batches
        if not all_dfs:
            self._logger.warning("No data returned from any batch")
            return pd.DataFrame(columns=["pixel_index", "timestamp", "value"])
        
        combined = pd.concat(all_dfs, ignore_index=True)
        self._logger.info(f"Combined {len(combined)} pixel results from {len(all_dfs)} batches")
        
        return combined
    
    def _parse_radar_response(self, response: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Parse API response into DataFrame.
        
        New API format returns:
        {
            "pixelIndex": 232229,
            "startTime": "2026-01-20T00:00:00Z",
            "endTime": "2026-01-21T00:00:00Z", 
            "dataOffsetSeconds": 60,
            "values": [0.05, 0.04, ...]
        }
        
        Args:
            response: API response (list of pixel data dicts)
            
        Returns:
            DataFrame with columns: ['pixel_index', 'timestamp', 'value']
        """
        if not response:
            return pd.DataFrame(columns=["pixel_index", "timestamp", "value"])
        
        from datetime import datetime, timedelta
        
        rows = []
        for pixel_data in response:
            pixel_index = pixel_data.get("pixelIndex")
            start_time_str = pixel_data.get("startTime")
            offset_seconds = pixel_data.get("dataOffsetSeconds", 60)
            values = pixel_data.get("values", [])
            
            if not values or not start_time_str:
                continue
            
            # Parse start time
            try:
                # Handle ISO format with Z suffix
                start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
            except ValueError:
                self._logger.warning(f"Failed to parse startTime: {start_time_str}")
                continue
            
            # Generate timestamps for each value
            for i, val in enumerate(values):
                timestamp = start_time + timedelta(seconds=i * offset_seconds)
                rows.append({
                    "pixel_index": pixel_index,
                    "timestamp": timestamp.isoformat(),
                    "value": val
                })
        
        return pd.DataFrame(rows)
    
    def organize_by_pixel(
        self,
        df: pd.DataFrame
    ) -> Dict[int, pd.DataFrame]:
        """
        Organize DataFrame by pixel index.
        
        Args:
            df: Combined radar data DataFrame
            
        Returns:
            Dictionary mapping pixel_index -> DataFrame
            
        Example:
            >>> by_pixel = fetcher.organize_by_pixel(df)
            >>> pixel_1_data = by_pixel[1]
        """
        if df.empty:
            return {}
        
        organized = {}
        for pixel_idx, group in df.groupby("pixel_index"):
            organized[pixel_idx] = group.reset_index(drop=True)
        
        return organized
