
from __future__ import annotations
import logging
import pickle
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

from ..moata.radar_client import RadarClient
from ..common.file_utils import ensure_dir

logger = logging.getLogger(__name__)


def datetime_to_iso_utc(dt: datetime) -> str:
    """Convert datetime to ISO format UTC string with Z suffix"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace('+00:00', 'Z')


class RadarDataCollector:
    """
    Collects rain radar (QPE) data for stormwater catchments
    
    Implements workflow from Sam's email:
    1. Get stormwater catchments with geometries
    2. Map pixels to each catchment (cached)
    3. Download radar data with batching
    """
    
    def __init__(
        self,
        radar_client: RadarClient,
        project_id: int,
        output_dir: str | Path,
        pixel_batch_size: int = 50,
        max_hours_per_request: int = 24
    ):
        self.radar_client = radar_client
        self.project_id = project_id
        self.output_dir = Path(output_dir)
        self.pixel_batch_size = pixel_batch_size
        self.max_hours = max_hours_per_request
        
        # Create output structure
        self.catchments_dir = self.output_dir / 'catchments'
        self.mappings_dir = self.output_dir / 'pixel_mappings'
        self.data_dir = self.output_dir / 'radar_data'
        
        for dir_path in [self.catchments_dir, self.mappings_dir, self.data_dir]:
            ensure_dir(dir_path)
    
    def get_catchments(self, force_refresh: bool = False) -> pd.DataFrame:
        """Get stormwater catchments with geometries"""
        cache_file = self.catchments_dir / 'stormwater_catchments.csv'
        
        if not force_refresh and cache_file.exists():
            logger.info(f'Loading cached catchments from {cache_file}')
            return pd.read_csv(cache_file)
        
        logger.info(f'Fetching stormwater catchments for project {self.project_id}')
        catchments = self.radar_client.get_stormwater_catchments(self.project_id)
        
        df = pd.DataFrame(catchments)
        logger.info(f'Found {len(df)} stormwater catchments')
        
        df.to_csv(cache_file, index=False)
        logger.info(f'Saved catchments to {cache_file}')
        
        return df
    
    def get_pixel_mappings(
        self,
        catchments_df: pd.DataFrame,
        force_refresh: bool = False
    ) -> Dict[int, List[int]]:
        """Get pixel mappings for all catchments"""
        cache_file = self.mappings_dir / 'catchment_pixel_mapping.pkl'
        
        if not force_refresh and cache_file.exists():
            logger.info(f'Loading cached pixel mappings from {cache_file}')
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        
        logger.info('Generating pixel mappings for all catchments...')
        catchment_to_pixels = {}
        
        for idx, row in catchments_df.iterrows():
            catchment_id = int(row['id'])
            catchment_name = row['name']
            geometry_wkt = row['geometryWkt']
            
            logger.info(f'Mapping pixels for: {catchment_name} (ID: {catchment_id})')
            
            try:
                pixel_mappings = self.radar_client.get_pixel_mappings_for_geometry(
                    geometry_wkt=geometry_wkt
                )
                
                pixel_indexes = [pm['pixelIndex'] for pm in pixel_mappings]
                catchment_to_pixels[catchment_id] = pixel_indexes
                
                logger.info(f'  Found {len(pixel_indexes)} pixels')
                
            except Exception as e:
                logger.error(f'Error getting pixels for catchment {catchment_id}: {e}')
                catchment_to_pixels[catchment_id] = []
        
        with open(cache_file, 'wb') as f:
            pickle.dump(catchment_to_pixels, f)
        logger.info(f'Saved pixel mappings to {cache_file}')
        
        json_file = self.mappings_dir / 'catchment_pixel_mapping.json'
        with open(json_file, 'w') as f:
            json.dump({str(k): v for k, v in catchment_to_pixels.items()}, f, indent=2)
        logger.info(f'Saved pixel mappings (JSON) to {json_file}')
        
        return catchment_to_pixels
    
    def download_catchment_data(
        self,
        catchment_id: int,
        pixel_indexes: List[int],
        start_time: datetime,
        end_time: datetime
    ) -> pd.DataFrame:
        """Download radar data for a single catchment"""
        logger.info(f'Downloading radar data for catchment {catchment_id}')
        logger.info(f'  Pixels: {len(pixel_indexes)}')
        logger.info(f'  Time range: {start_time} to {end_time}')
        
        if not pixel_indexes:
            logger.warning(f'No pixels for catchment {catchment_id}')
            return pd.DataFrame()
        
        time_chunks = self._create_time_chunks(start_time, end_time)
        logger.info(f'  Time chunks: {len(time_chunks)}')
        
        all_data = []
        
        for chunk_idx, (chunk_start, chunk_end) in enumerate(time_chunks, 1):
            logger.info(f'  Processing time chunk {chunk_idx}/{len(time_chunks)}')
            
            start_iso = datetime_to_iso_utc(chunk_start)
            end_iso = datetime_to_iso_utc(chunk_end)
            
            try:
                chunk_data = self.radar_client.batch_get_radar_data(
                    pixel_indexes=pixel_indexes,
                    start_time=start_iso,
                    end_time=end_iso,
                    batch_size=self.pixel_batch_size
                )
                
                all_data.extend(chunk_data)
                logger.info(f'    Retrieved {len(chunk_data)} records')
                
            except Exception as e:
                logger.error(f'Error downloading chunk {chunk_idx}: {e}')
        
        if not all_data:
            logger.warning(f'No data retrieved for catchment {catchment_id}')
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        logger.info(f'Total records: {len(df)}')
        
        return df
    
    def download_all_catchments(
        self,
        start_time: datetime,
        end_time: datetime,
        catchment_ids: Optional[List[int]] = None,
        force_refresh_pixels: bool = False
    ) -> Dict[int, pd.DataFrame]:
        """Download radar data for multiple catchments"""
        logger.info('='*80)
        logger.info('RADAR DATA COLLECTION - FULL WORKFLOW')
        logger.info('='*80)
        
        logger.info('\nSTEP 1: Getting catchments')
        catchments_df = self.get_catchments()
        
        logger.info('\nSTEP 2: Mapping pixels to catchments')
        catchment_to_pixels = self.get_pixel_mappings(
            catchments_df,
            force_refresh=force_refresh_pixels
        )
        
        logger.info('\nPixel Mapping Summary:')
        for cid, pixels in catchment_to_pixels.items():
            name = catchments_df[catchments_df['id'] == cid]['name'].values[0]
            logger.info(f'  {name} (ID: {cid}): {len(pixels)} pixels')
        
        logger.info('\nSTEP 3: Downloading radar data')
        
        if catchment_ids:
            target_catchments = {k: v for k, v in catchment_to_pixels.items() if k in catchment_ids}
        else:
            target_catchments = catchment_to_pixels
        
        results = {}
        
        for catchment_id, pixel_indexes in target_catchments.items():
            df = self.download_catchment_data(
                catchment_id=catchment_id,
                pixel_indexes=pixel_indexes,
                start_time=start_time,
                end_time=end_time
            )
            
            if not df.empty:
                output_file = self.data_dir / f'catchment_{catchment_id}_radar_data.csv'
                df.to_csv(output_file, index=False)
                logger.info(f'Saved to {output_file}')
                
                results[catchment_id] = df
        
        logger.info('\n' + '='*80)
        logger.info('COLLECTION COMPLETE!')
        logger.info('='*80)
        
        return results
    
    def _create_time_chunks(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """Split time range into chunks (max 24 hours each)"""
        chunks = []
        current_start = start_time
        
        while current_start < end_time:
            current_end = min(
                current_start + timedelta(hours=self.max_hours),
                end_time
            )
            chunks.append((current_start, current_end))
            current_start = current_end
        
        return chunks