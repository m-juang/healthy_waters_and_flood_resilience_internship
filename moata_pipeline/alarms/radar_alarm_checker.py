"""
Real-time Radar Alarm Checker

Checks radar data against ARI thresholds using LATEST window only.

Key Difference from Analysis:
- ANALYSIS: Find maximum ARI across entire time period (24h)
- ALARMS: Check only most recent window for each duration

This implements Sam's requirement:
"Alarms are only considering the rainfall relative to the time they are checked,
i.e. for a check that runs at 14:00, it takes the totals from 13:50-14:00 for
the 10 minute duration, 13:40-14:00 for the 20 minute duration etc."

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23
Version: 2.4.0 - Optimized: Pre-load TP108 coefficients once before threading
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from moata_pipeline.analyze.ari_calculator import ARICalculator, DURATION_CONFIG


__version__ = "2.4.0"


def _get_rainfall_column(df: pd.DataFrame) -> str:
    """Determine which column to use for rainfall values."""
    if "weighted_value" in df.columns:
        return "weighted_value"
    elif "value" in df.columns:
        return "value"
    else:
        raise ValueError(
            f"DataFrame must have 'weighted_value' or 'value' column. "
            f"Found: {df.columns.tolist()}"
        )


class RadarAlarmChecker:
    """
    Check radar data for alarm conditions at specific timestamps.
    
    Supports both sequential and threaded processing.
    ThreadPoolExecutor is recommended for faster processing on Windows.
    
    OPTIMIZED (v2.4.0): TP108 coefficients are pre-loaded once and shared
    across all threads for maximum performance.
    """
    
    def __init__(
        self,
        tp108_path: Path,
        ari_threshold: float = 5.0,
        area_threshold: float = 0.25,
        preload_coefficients: bool = True,
    ):
        """
        Initialize alarm checker with thresholds.
        
        Args:
            tp108_path: Path to TP108 coefficients CSV
            ari_threshold: ARI threshold in years (default: 5.0)
            area_threshold: Proportion of pixels that must exceed threshold (default: 0.25 = 25%)
            preload_coefficients: If True, load coefficients immediately (default: True)
        """
        self.calc = ARICalculator(tp108_path, ari_threshold)
        self.ari_threshold = ari_threshold
        self.area_threshold = area_threshold
        self.tp108_path = tp108_path
        self.logger = logging.getLogger(__name__)
        
        # Pre-loaded coefficients cache (shared across threads)
        self._coefficients_cache: Optional[pd.DataFrame] = None
        
        # Pre-load coefficients if requested
        if preload_coefficients:
            self._preload_coefficients()
    
    def _preload_coefficients(self) -> None:
        """Pre-load TP108 coefficients into cache."""
        if self._coefficients_cache is None:
            self.logger.info("Pre-loading TP108 coefficients...")
            self._coefficients_cache = self.calc.load_coefficients()
            self.logger.info(f"✓ Coefficients cached for {len(self._coefficients_cache)} pixels")
    
    def _get_coefficients(self) -> pd.DataFrame:
        """Get coefficients from cache or load if needed."""
        if self._coefficients_cache is not None:
            return self._coefficients_cache
        return self.calc.load_coefficients()
    
    def check_catchment_at_time(
        self,
        catchment_df: pd.DataFrame,
        check_time: datetime,
        catchment_id: Optional[int] = None,
        catchment_name: Optional[str] = None,
    ) -> Dict:
        """
        Check if catchment triggers alarm at specific timestamp.
        
        Args:
            catchment_df: DataFrame with pixel rainfall data
            check_time: Timestamp to check
            catchment_id: Optional catchment ID
            catchment_name: Optional catchment name
            
        Returns:
            Dict with alarm status and details
        """
        # Validate input
        if "pixel_index" not in catchment_df.columns:
            raise ValueError("DataFrame must have 'pixel_index' column")
        if "timestamp" not in catchment_df.columns:
            raise ValueError("DataFrame must have 'timestamp' column")
        
        # Determine rainfall column
        rainfall_col = _get_rainfall_column(catchment_df)
        used_weighted = rainfall_col == "weighted_value"
        
        if used_weighted:
            self.logger.debug(f"Using weighted_value for {catchment_name} (area-proportional)")
        else:
            self.logger.debug(f"Using raw value for {catchment_name} (no weights available)")
        
        # Prepare data
        catchment_df = catchment_df.copy()
        catchment_df["timestamp"] = pd.to_datetime(catchment_df["timestamp"])
        
        # Get coefficients from cache
        coeffs = self._get_coefficients()
        
        # Get all pixels
        pixels = catchment_df["pixel_index"].unique()
        pixels_exceeding: Set[int] = set()
        exceedance_details = []
        
        # Check each pixel
        for pixel_idx in pixels:
            # Skip if no coefficients
            if pixel_idx not in coeffs.index:
                continue
            
            # Get pixel data
            pixel_data = catchment_df[catchment_df["pixel_index"] == pixel_idx].copy()
            pixel_data = pixel_data.sort_values("timestamp").set_index("timestamp")
            pixel_coeffs = coeffs.loc[pixel_idx]
            
            pixel_exceeds = False
            
            # Check each duration AT CHECK_TIME ONLY
            for dur_name, minutes in DURATION_CONFIG.items():
                b_col = f"{dur_name}_b"
                m_col = f"{dur_name}_m"
                
                # Skip if coefficients missing
                if b_col not in pixel_coeffs or m_col not in pixel_coeffs:
                    continue
                
                b = pixel_coeffs[b_col]
                m = pixel_coeffs[m_col]
                
                if pd.isna(b) or pd.isna(m):
                    continue
                
                # Calculate depth for LATEST WINDOW ONLY
                window_start = check_time - timedelta(minutes=minutes)
                
                window_data = pixel_data[
                    (pixel_data.index >= window_start) &
                    (pixel_data.index <= check_time)
                ]
                
                if len(window_data) == 0:
                    continue
                
                depth = window_data[rainfall_col].sum()
                
                if depth <= 0:
                    continue
                
                # Calculate ARI
                ari = self.calc.calculate_ari(depth, b, m)
                
                # Check threshold
                if ari >= self.ari_threshold:
                    pixel_exceeds = True
                    exceedance_details.append({
                        "pixel_index": int(pixel_idx),
                        "duration": dur_name,
                        "depth_mm": round(depth, 2),
                        "ari_years": round(ari, 2),
                    })
            
            if pixel_exceeds:
                pixels_exceeding.add(pixel_idx)
        
        # Calculate proportion
        proportion = len(pixels_exceeding) / len(pixels) if len(pixels) > 0 else 0
        alarm_triggered = proportion >= self.area_threshold
        
        return {
            "timestamp": check_time,
            "catchment_id": catchment_id,
            "catchment_name": catchment_name,
            "alarm": alarm_triggered,
            "pixels_total": len(pixels),
            "pixels_exceeding": len(pixels_exceeding),
            "proportion": round(proportion, 4),
            "exceeding_details": exceedance_details,
            "used_weighted": used_weighted,
        }
    
    def check_catchment_timeline(
        self,
        catchment_csv: Path,
        catchment_id: Optional[int] = None,
        catchment_name: Optional[str] = None,
        quiet: bool = False,
    ) -> pd.DataFrame:
        """
        Check alarm status at EVERY timestamp in catchment data.
        
        This is the main method used by check_alarm_timeline.py script.
        
        Args:
            catchment_csv: Path to catchment radar CSV file
            catchment_id: Optional catchment ID
            catchment_name: Optional catchment name
            quiet: If True, suppress progress logging (for threaded mode)
            
        Returns:
            DataFrame with alarm status at each timestamp
        """
        # Load data
        df = pd.read_csv(catchment_csv)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        rainfall_col = _get_rainfall_column(df)
        used_weighted = rainfall_col == "weighted_value"
        
        if not quiet:
            if used_weighted:
                self.logger.info(f"Using weighted_value for {catchment_name or catchment_csv.name} (area-proportional)")
            else:
                self.logger.info(f"Using raw value for {catchment_name or catchment_csv.name} (no weights available)")
        
        timestamps = sorted(df["timestamp"].unique())
        
        if not quiet:
            self.logger.info(
                f"Checking {len(timestamps)} timestamps for {catchment_name or catchment_csv.name}"
            )
        
        # Get coefficients from cache (NOT loading again!)
        coeffs = self._get_coefficients()
        
        pixels = df["pixel_index"].unique()
        total_pixels = len(pixels)
        
        timestamp_exceeding: Dict[pd.Timestamp, Set[int]] = {ts: set() for ts in timestamps}
        
        # Process each pixel once, check all timestamps
        for i, pixel_idx in enumerate(pixels):
            if not quiet and i % 25 == 0 and i > 0:
                self.logger.info(f"  Progress: Processing pixel {i}/{total_pixels}")
            
            if pixel_idx not in coeffs.index:
                continue
            
            pixel_data = df[df["pixel_index"] == pixel_idx].copy()
            pixel_data = pixel_data.sort_values("timestamp").set_index("timestamp")
            pixel_coeffs = coeffs.loc[pixel_idx]
            
            # PRE-COMPUTE rolling sums for efficiency
            rolling_sums = {}
            for dur_name, minutes in DURATION_CONFIG.items():
                b_col = f"{dur_name}_b"
                m_col = f"{dur_name}_m"
                
                if b_col not in pixel_coeffs or m_col not in pixel_coeffs:
                    continue
                
                b = pixel_coeffs[b_col]
                m = pixel_coeffs[m_col]
                
                if pd.isna(b) or pd.isna(m):
                    continue
                
                rolling = pixel_data[rainfall_col].rolling(
                    window=minutes,
                    min_periods=minutes
                ).sum()
                
                rolling_sums[dur_name] = (rolling, b, m)
            
            # Check timestamps
            for ts in timestamps:
                if ts not in pixel_data.index:
                    continue
                
                pixel_exceeds = False
                
                for dur_name, (rolling, b, m) in rolling_sums.items():
                    if ts not in rolling.index:
                        continue
                    
                    depth = rolling.loc[ts]
                    
                    if pd.isna(depth) or depth <= 0:
                        continue
                    
                    ari = self.calc.calculate_ari(depth, b, m)
                    
                    if ari >= self.ari_threshold:
                        pixel_exceeds = True
                        break
                
                if pixel_exceeds:
                    timestamp_exceeding[ts].add(pixel_idx)
        
        # Build results
        results = []
        for ts in timestamps:
            pixels_exceeding = len(timestamp_exceeding[ts])
            proportion = pixels_exceeding / total_pixels if total_pixels > 0 else 0
            alarm = proportion >= self.area_threshold
            
            results.append({
                "timestamp": ts,
                "catchment_id": catchment_id,
                "catchment_name": catchment_name,
                "alarm": alarm,
                "pixels_total": total_pixels,
                "pixels_exceeding": pixels_exceeding,
                "proportion": round(proportion, 4),
                "used_weighted": used_weighted,
            })
        
        if not quiet:
            self.logger.info(f"✓ Completed timeline check for {catchment_name}")
        
        return pd.DataFrame(results)
    
    def _process_single_catchment(
        self,
        filepath: Path,
        index: int,
        total: int,
    ) -> Tuple[str, Optional[pd.DataFrame], bool]:
        """
        Process a single catchment file (for threaded execution).
        
        Note: This method uses the pre-loaded coefficients cache from self.
        
        Args:
            filepath: Path to catchment CSV file
            index: Current index (1-based)
            total: Total number of catchments
            
        Returns:
            Tuple of (catchment_name, timeline_df or None, has_alarms)
        """
        # Extract catchment info
        parts = filepath.stem.split("_", 1)
        catchment_id = int(parts[0]) if parts[0].isdigit() else None
        catchment_name = parts[1] if len(parts) > 1 else filepath.stem
        
        try:
            # Process with quiet=True to reduce log noise in threaded mode
            # Coefficients are already cached in self._coefficients_cache
            timeline_df = self.check_catchment_timeline(
                catchment_csv=filepath,
                catchment_id=catchment_id,
                catchment_name=catchment_name,
                quiet=True,
            )
            
            alarm_count = timeline_df['alarm'].sum()
            has_alarms = alarm_count > 0
            
            return (catchment_name, timeline_df, has_alarms)
            
        except Exception as e:
            self.logger.error(f"Error processing {catchment_name}: {e}")
            return (catchment_name, None, False)
    
    def check_multiple_catchments_threaded(
        self,
        catchment_files: List[Path],
        max_workers: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> pd.DataFrame:
        """
        Check alarm timeline for multiple catchments using ThreadPoolExecutor.
        
        This is FASTER than sequential and SAFER than multiprocessing on Windows.
        
        OPTIMIZED (v2.4.0): TP108 coefficients are pre-loaded once before
        threading starts, eliminating redundant file reads.
        
        Args:
            catchment_files: List of catchment CSV file paths
            max_workers: Number of threads (default: min(8, cpu_count))
            progress_callback: Optional callback(completed, total, catchment_name, has_alarms)
            
        Returns:
            Combined DataFrame with all catchment timelines
        """
        import os
        
        if max_workers is None:
            max_workers = min(8, os.cpu_count() or 4)
        
        total = len(catchment_files)
        
        # Ensure coefficients are pre-loaded before threading
        self._preload_coefficients()
        
        self.logger.info(f"Processing {total} catchments using {max_workers} threads")
        self.logger.info("")
        
        all_timelines = []
        completed = 0
        alarms_found = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            # Note: self is passed implicitly, carrying the pre-loaded coefficients
            future_to_file = {
                executor.submit(
                    self._process_single_catchment,
                    filepath,
                    i,
                    total
                ): filepath
                for i, filepath in enumerate(catchment_files, 1)
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                filepath = future_to_file[future]
                completed += 1
                
                try:
                    catchment_name, timeline_df, has_alarms = future.result()
                    
                    if timeline_df is not None:
                        all_timelines.append(timeline_df)
                        
                        if has_alarms:
                            alarms_found += 1
                            self.logger.info(
                                f"[{completed}/{total}] ✓ {catchment_name} - 🚨 ALARMS DETECTED"
                            )
                        else:
                            self.logger.info(
                                f"[{completed}/{total}] ✓ {catchment_name} - No alarms"
                            )
                    else:
                        self.logger.warning(
                            f"[{completed}/{total}] ❌ {catchment_name} - Failed"
                        )
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(completed, total, catchment_name, has_alarms)
                        
                except Exception as e:
                    self.logger.error(f"[{completed}/{total}] ❌ Error: {e}")
        
        self.logger.info("")
        self.logger.info(f"✓ Threaded processing complete")
        self.logger.info(f"  Successful: {len(all_timelines)}/{total}")
        self.logger.info(f"  With alarms: {alarms_found}")
        
        if all_timelines:
            return pd.concat(all_timelines, ignore_index=True)
        else:
            self.logger.warning("No catchments were successfully processed")
            return pd.DataFrame()