"""
ARI (Annual Recurrence Interval) Calculator for Radar Data

Uses TP108 coefficients to convert rainfall totals to ARI values.
Formula: ARI = exp(m * D + b)

Where:
- D = rainfall depth (mm) for a given duration
- m, b = coefficients from tp108_stats.csv per pixel and duration
"""
from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Duration mapping: column prefix -> minutes
DURATION_CONFIG = {
    "10m": 10,
    "20m": 20,
    "30m": 30,
    "60m": 60,
    "2h": 120,
    "6h": 360,
    "12h": 720,
    "24h": 1440,
}


class ARICalculator:
    """Calculate ARI values from radar rainfall data using TP108 coefficients."""
    
    def __init__(
        self,
        tp108_path: Path = Path("data/inputs/tp108_stats.csv"),
        ari_threshold: float = 5.0,
    ):
        """
        Initialize ARI calculator.
        
        Args:
            tp108_path: Path to TP108 coefficients CSV
            ari_threshold: Minimum ARI to record (default 5.0 years)
        """
        self._tp108_path = Path(tp108_path)
        self._ari_threshold = ari_threshold
        self._coefficients: Optional[pd.DataFrame] = None
    
    def load_coefficients(self) -> pd.DataFrame:
        """Load TP108 coefficients from CSV."""
        if self._coefficients is None:
            logger.info("Loading TP108 coefficients from %s", self._tp108_path)
            df = pd.read_csv(self._tp108_path)
            self._coefficients = df.set_index("pixelindex")
            logger.info("✓ Loaded coefficients for %d pixels", len(self._coefficients))
        return self._coefficients
    
    @staticmethod
    def calculate_ari(depth: float, b: float, m: float) -> float:
        """
        Calculate ARI from rainfall depth.
        
        Formula: ARI = exp(m * D + b)
        
        Args:
            depth: Rainfall depth in mm
            b: Intercept coefficient
            m: Slope coefficient
            
        Returns:
            ARI value in years
        """
        if depth <= 0 or pd.isna(depth):
            return 0.0
        try:
            return math.exp(m * depth + b)
        except OverflowError:
            return float('inf')
    
    @staticmethod
    def depth_for_ari(target_ari: float, b: float, m: float) -> float:
        """
        Calculate rainfall depth required to achieve a target ARI.
        
        Formula: D = (ln(ARI) - b) / m
        
        Args:
            target_ari: Target ARI in years
            b: Intercept coefficient
            m: Slope coefficient
            
        Returns:
            Required depth in mm
        """
        if target_ari <= 0 or m == 0:
            return float('inf')
        return (math.log(target_ari) - b) / m
    
    def process_pixel_data(
        self,
        pixel_df: pd.DataFrame,
        pixel_index: int,
    ) -> List[Dict[str, Any]]:
        """
        Process rainfall data for a single pixel and calculate ARI.
        
        Args:
            pixel_df: DataFrame with 'timestamp' and 'value' columns, sorted by timestamp
            pixel_index: Pixel index for coefficient lookup
            
        Returns:
            List of ARI exceedance records
        """
        coeffs = self.load_coefficients()
        
        if pixel_index not in coeffs.index:
            return []
        
        pixel_coeffs = coeffs.loc[pixel_index]
        results = []
        
        # Ensure timestamp is index for rolling
        if "timestamp" in pixel_df.columns:
            pixel_df = pixel_df.set_index("timestamp").sort_index()
        
        for duration_name, minutes in DURATION_CONFIG.items():
            b_col = f"{duration_name}_b"
            m_col = f"{duration_name}_m"
            
            if b_col not in pixel_coeffs or m_col not in pixel_coeffs:
                continue
            
            b = pixel_coeffs[b_col]
            m = pixel_coeffs[m_col]
            
            if pd.isna(b) or pd.isna(m):
                continue
            
            # Rolling sum over duration window
            rolling_sum = pixel_df["value"].rolling(
                window=minutes,
                min_periods=minutes,
            ).sum()
            
            # Calculate ARI for each timestamp
            for ts, depth in rolling_sum.items():
                if pd.isna(depth) or depth <= 0:
                    continue
                
                ari = self.calculate_ari(depth, b, m)
                
                if ari >= self._ari_threshold:
                    results.append({
                        "pixel_index": pixel_index,
                        "timestamp": ts,
                        "duration": duration_name,
                        "duration_minutes": minutes,
                        "rainfall_depth_mm": round(depth, 2),
                        "ari_years": round(ari, 2),
                    })
        
        return results
    
    def process_catchment_file(
        self,
        radar_csv: Path,
        output_csv: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Process radar data file for a catchment and calculate ARI values.
        
        Args:
            radar_csv: Path to radar data CSV
            output_csv: Optional path to save results
            
        Returns:
            DataFrame with ARI exceedance records
        """
        logger.info("Processing %s", radar_csv.name)
        
        # Load radar data
        df = pd.read_csv(radar_csv)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Get unique pixels
        pixels = df["pixel_index"].unique()
        logger.info("  Processing %d pixels", len(pixels))
        
        all_results = []
        
        for pixel_index in pixels:
            pixel_data = df[df["pixel_index"] == pixel_index].copy()
            results = self.process_pixel_data(pixel_data, pixel_index)
            all_results.extend(results)
        
        result_df = pd.DataFrame(all_results)
        
        if output_csv and not result_df.empty:
            output_csv.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(output_csv, index=False)
            logger.info("  ✓ Saved %d ARI records to %s", len(result_df), output_csv.name)
        elif result_df.empty:
            logger.info("  No ARI exceedances found (threshold=%.1f years)", self._ari_threshold)
        
        return result_df
    
    def get_max_ari_summary(self, ari_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get maximum ARI per timestamp across all pixels and durations.
        
        Args:
            ari_df: DataFrame from process_catchment_file()
            
        Returns:
            DataFrame with max ARI per timestamp
        """
        if ari_df.empty:
            return pd.DataFrame()
        
        # Find row with max ARI for each timestamp
        idx = ari_df.groupby("timestamp")["ari_years"].idxmax()
        summary = ari_df.loc[idx].copy()
        
        return summary.sort_values("timestamp").reset_index(drop=True)
    
    def get_catchment_peak_ari(self, ari_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get peak ARI statistics for a catchment.
        
        Args:
            ari_df: DataFrame from process_catchment_file()
            
        Returns:
            Dict with peak statistics
        """
        if ari_df.empty:
            return {
                "peak_ari": 0,
                "peak_timestamp": None,
                "peak_duration": None,
                "peak_depth_mm": None,
                "exceedance_count": 0,
            }
        
        peak_idx = ari_df["ari_years"].idxmax()
        peak_row = ari_df.loc[peak_idx]
        
        return {
            "peak_ari": peak_row["ari_years"],
            "peak_timestamp": peak_row["timestamp"],
            "peak_duration": peak_row["duration"],
            "peak_depth_mm": peak_row["rainfall_depth_mm"],
            "peak_pixel": peak_row["pixel_index"],
            "exceedance_count": len(ari_df),
            "pixels_with_exceedance": ari_df["pixel_index"].nunique(),
        }


def process_all_catchments(
    radar_dir: Path = Path("outputs/rain_radar/raw/radar_data"),
    output_dir: Path = Path("outputs/rain_radar/ari"),
    tp108_path: Path = Path("data/inputs/tp108_stats.csv"),
    ari_threshold: float = 5.0,
) -> pd.DataFrame:
    """
    Process all catchment radar files and calculate ARI.
    
    Args:
        radar_dir: Directory with radar CSV files
        output_dir: Directory for ARI output files
        tp108_path: Path to TP108 coefficients
        ari_threshold: Minimum ARI to record
        
    Returns:
        Summary DataFrame with peak ARI per catchment
    """
    calc = ARICalculator(tp108_path=tp108_path, ari_threshold=ari_threshold)
    
    radar_files = list(radar_dir.glob("*.csv"))
    logger.info("Found %d radar data files", len(radar_files))
    
    summaries = []
    
    for radar_file in radar_files:
        # Extract catchment info from filename
        parts = radar_file.stem.split("_", 1)
        catchment_id = int(parts[0]) if parts[0].isdigit() else None
        catchment_name = parts[1] if len(parts) > 1 else radar_file.stem
        
        # Process file
        output_csv = output_dir / f"ari_{radar_file.name}"
        ari_df = calc.process_catchment_file(radar_file, output_csv)
        
        # Get summary
        peak = calc.get_catchment_peak_ari(ari_df)
        peak["catchment_id"] = catchment_id
        peak["catchment_name"] = catchment_name
        summaries.append(peak)
    
    summary_df = pd.DataFrame(summaries)
    
    # Save summary
    summary_path = output_dir / "ari_summary.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(summary_path, index=False)
    logger.info("✓ Saved ARI summary to %s", summary_path)
    
    return summary_df
