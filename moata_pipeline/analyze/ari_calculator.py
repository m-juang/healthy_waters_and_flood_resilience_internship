"""
ARI (Annual Recurrence Interval) Calculator Module

Calculates ARI values from radar rainfall data using TP108 coefficients.

Formula: ARI = exp(m * D + b)

Where:
    - D = rainfall depth (mm) for a given duration
    - m, b = coefficients from tp108_stats.csv per pixel and duration

Classes:
    ARICalculator: Main calculator class with TP108 coefficient handling

Functions:
    process_all_catchments: Batch process all catchment files

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# Version info
__version__ = "1.0.0"


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


# =============================================================================
# Custom Exceptions
# =============================================================================

class ARICalculationError(Exception):
    """Base exception for ARI calculation errors."""
    pass


class CoefficientsNotFoundError(ARICalculationError):
    """Raised when TP108 coefficients file not found."""
    pass


class InvalidDataError(ARICalculationError):
    """Raised when input data is invalid."""
    pass


# =============================================================================
# ARI Calculator Class
# =============================================================================

class ARICalculator:
    """
    Calculate ARI values from radar rainfall data using TP108 coefficients.
    
    Uses the TP108 methodology to convert rainfall depths to ARI (Annual
    Recurrence Interval) values for various durations (10m, 20m, 30m, 1h, 2h, 6h, 12h, 24h).
    
    Args:
        tp108_path: Path to TP108 coefficients CSV file
        ari_threshold: Minimum ARI value to record (default: 5.0 years)
        
    Example:
        >>> calc = ARICalculator(
        ...     tp108_path=Path("data/inputs/tp108_stats.csv"),
        ...     ari_threshold=5.0
        ... )
        >>> calc.load_coefficients()
        >>> ari_df = calc.process_catchment_file(radar_csv)
    """
    
    def __init__(
        self,
        tp108_path: Path = Path("data/inputs/tp108_stats.csv"),
        ari_threshold: float = 5.0,
    ) -> None:
        """
        Initialize ARI calculator.
        
        Args:
            tp108_path: Path to TP108 coefficients CSV
            ari_threshold: Minimum ARI to record (default: 5.0 years)
            
        Raises:
            ValueError: If ari_threshold is invalid
        """
        if ari_threshold <= 0:
            raise ValueError(f"ari_threshold must be positive, got {ari_threshold}")
        
        self._tp108_path = Path(tp108_path)
        self._ari_threshold = ari_threshold
        self._coefficients: Optional[pd.DataFrame] = None
        self._logger = logging.getLogger(f"{__name__}.ARICalculator")
    
    def load_coefficients(self) -> pd.DataFrame:
        """
        Load TP108 coefficients from CSV file.
        
        Returns:
            DataFrame with coefficients indexed by pixelindex
            
        Raises:
            CoefficientsNotFoundError: If coefficients file not found
            InvalidDataError: If coefficients file is invalid
        """
        if self._coefficients is not None:
            return self._coefficients
        
        if not self._tp108_path.exists():
            raise CoefficientsNotFoundError(
                f"TP108 coefficients file not found: {self._tp108_path}\n\n"
                f"This file is required for ARI calculations.\n"
                f"Expected location: data/inputs/tp108_stats.csv\n"
                f"Contact your supervisor for the TP108 coefficient file."
            )
        
        try:
            self._logger.info(f"Loading TP108 coefficients from {self._tp108_path}")
            df = pd.read_csv(self._tp108_path)
            
            # Validate required column
            if "pixelindex" not in df.columns:
                raise InvalidDataError(
                    f"TP108 coefficients missing 'pixelindex' column.\n"
                    f"Found columns: {df.columns.tolist()}"
                )
            
            self._coefficients = df.set_index("pixelindex")
            self._logger.info(f"✓ Loaded coefficients for {len(self._coefficients)} pixels")
            
            return self._coefficients
            
        except pd.errors.EmptyDataError:
            raise InvalidDataError(
                f"TP108 coefficients file is empty: {self._tp108_path}"
            )
        except Exception as e:
            raise InvalidDataError(
                f"Failed to load TP108 coefficients: {e}"
            ) from e
    
    @staticmethod
    def calculate_ari(depth: float, b: float, m: float) -> float:
        """
        Calculate ARI from rainfall depth using TP108 formula.
        
        Formula: ARI = exp(m * D + b)
        
        Args:
            depth: Rainfall depth in mm
            b: Intercept coefficient
            m: Slope coefficient
            
        Returns:
            ARI value in years (0.0 if invalid input)
            
        Example:
            >>> ARICalculator.calculate_ari(depth=50.0, b=1.5, m=0.02)
            8.17
        """
        if depth <= 0 or pd.isna(depth):
            return 0.0
        
        if pd.isna(b) or pd.isna(m):
            return 0.0
        
        try:
            return math.exp(m * depth + b)
        except OverflowError:
            # Very large ARI values
            return float('inf')
        except Exception:
            return 0.0
    
    @staticmethod
    def depth_for_ari(target_ari: float, b: float, m: float) -> float:
        """
        Calculate rainfall depth required to achieve a target ARI.
        
        Inverse formula: D = (ln(ARI) - b) / m
        
        Args:
            target_ari: Target ARI in years
            b: Intercept coefficient
            m: Slope coefficient
            
        Returns:
            Required depth in mm
            
        Example:
            >>> ARICalculator.depth_for_ari(target_ari=10.0, b=1.5, m=0.02)
            62.66
        """
        if target_ari <= 0 or m == 0:
            return float('inf')
        
        if pd.isna(b) or pd.isna(m):
            return float('inf')
        
        try:
            return (math.log(target_ari) - b) / m
        except Exception:
            return float('inf')
    
    def process_pixel_data(
        self,
        pixel_df: pd.DataFrame,
        pixel_index: int,
    ) -> List[Dict[str, Any]]:
        """
        Process rainfall data for a single pixel and calculate ARI.
        
        Calculates rolling rainfall totals for each duration window and
        converts to ARI values using TP108 coefficients.
        
        Args:
            pixel_df: DataFrame with 'timestamp' and 'value' columns (sorted by timestamp)
            pixel_index: Pixel index for coefficient lookup
            
        Returns:
            List of ARI exceedance records (only values ≥ threshold)
            
        Raises:
            InvalidDataError: If pixel_df is missing required columns
        """
        # Validate input data
        if "value" not in pixel_df.columns:
            raise InvalidDataError(
                f"pixel_df must have 'value' column. Found: {pixel_df.columns.tolist()}"
            )
        
        # Load coefficients
        coeffs = self.load_coefficients()
        
        # Check if pixel has coefficients
        if pixel_index not in coeffs.index:
            self._logger.debug(f"No coefficients for pixel {pixel_index}")
            return []
        
        pixel_coeffs = coeffs.loc[pixel_index]
        results = []
        
        # Ensure timestamp is index for rolling calculations
        if "timestamp" in pixel_df.columns:
            pixel_df = pixel_df.set_index("timestamp").sort_index()
        
        # Process each duration
        for duration_name, minutes in DURATION_CONFIG.items():
            b_col = f"{duration_name}_b"
            m_col = f"{duration_name}_m"
            
            # Check if coefficients exist for this duration
            if b_col not in pixel_coeffs or m_col not in pixel_coeffs:
                continue
            
            b = pixel_coeffs[b_col]
            m = pixel_coeffs[m_col]
            
            if pd.isna(b) or pd.isna(m):
                continue
            
            # Calculate rolling sum over duration window
            rolling_sum = pixel_df["value"].rolling(
                window=minutes,
                min_periods=minutes,
            ).sum()
            
            # Calculate ARI for each timestamp
            for ts, depth in rolling_sum.items():
                if pd.isna(depth) or depth <= 0:
                    continue
                
                ari = self.calculate_ari(depth, b, m)
                
                # Only record if above threshold
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
            radar_csv: Path to radar data CSV (with pixel_index, timestamp, value)
            output_csv: Optional path to save results
            
        Returns:
            DataFrame with ARI exceedance records
            
        Raises:
            FileNotFoundError: If radar_csv doesn't exist
            InvalidDataError: If radar_csv has invalid structure
        """
        if not radar_csv.exists():
            raise FileNotFoundError(
                f"Radar data file not found: {radar_csv}\n\n"
                f"Run data collection first:\n"
                f"  python retrieve_rain_radar.py"
            )
        
        self._logger.info(f"Processing {radar_csv.name}")
        
        try:
            # Load radar data
            df = pd.read_csv(radar_csv)
            
            # Validate columns
            required_cols = ["pixel_index", "timestamp", "value"]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                raise InvalidDataError(
                    f"Radar CSV missing columns: {missing}\n"
                    f"Found: {df.columns.tolist()}"
                )
            
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
        except pd.errors.EmptyDataError:
            raise InvalidDataError(f"Radar CSV is empty: {radar_csv}")
        except Exception as e:
            raise InvalidDataError(f"Failed to load radar CSV: {e}") from e
        
        # Get unique pixels
        pixels = df["pixel_index"].unique()
        self._logger.info(f"  Processing {len(pixels)} pixels")
        
        all_results = []
        
        for pixel_index in pixels:
            pixel_data = df[df["pixel_index"] == pixel_index].copy()
            
            try:
                results = self.process_pixel_data(pixel_data, pixel_index)
                all_results.extend(results)
            except Exception as e:
                self._logger.warning(
                    f"  Failed to process pixel {pixel_index}: {e}"
                )
                continue
        
        result_df = pd.DataFrame(all_results)
        
        # Save results if requested
        if output_csv and not result_df.empty:
            output_csv.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(output_csv, index=False)
            self._logger.info(
                f"  ✓ Saved {len(result_df)} ARI records to {output_csv.name}"
            )
        elif result_df.empty:
            self._logger.info(
                f"  No ARI exceedances found (threshold={self._ari_threshold:.1f} years)"
            )
        
        return result_df
    
    def get_max_ari_summary(self, ari_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get maximum ARI per timestamp across all pixels and durations.
        
        Args:
            ari_df: DataFrame from process_catchment_file()
            
        Returns:
            DataFrame with max ARI per timestamp (sorted by timestamp)
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
            Dictionary with peak statistics:
                - peak_ari: Maximum ARI value
                - peak_timestamp: When peak occurred
                - peak_duration: Duration of peak event
                - peak_depth_mm: Rainfall depth at peak
                - peak_pixel: Pixel index where peak occurred
                - exceedance_count: Total ARI exceedances
                - pixels_with_exceedance: Number of pixels with exceedances
        """
        if ari_df.empty:
            return {
                "peak_ari": 0,
                "peak_timestamp": None,
                "peak_duration": None,
                "peak_depth_mm": None,
                "peak_pixel": None,
                "exceedance_count": 0,
                "pixels_with_exceedance": 0,
            }
        
        peak_idx = ari_df["ari_years"].idxmax()
        peak_row = ari_df.loc[peak_idx]
        
        return {
            "peak_ari": float(peak_row["ari_years"]),
            "peak_timestamp": peak_row["timestamp"],
            "peak_duration": peak_row["duration"],
            "peak_depth_mm": float(peak_row["rainfall_depth_mm"]),
            "peak_pixel": int(peak_row["pixel_index"]),
            "exceedance_count": len(ari_df),
            "pixels_with_exceedance": int(ari_df["pixel_index"].nunique()),
        }


# =============================================================================
# Batch Processing Function
# =============================================================================

def process_all_catchments(
    radar_dir: Path = Path("outputs/rain_radar/raw/radar_data"),
    output_dir: Path = Path("outputs/rain_radar/ari"),
    tp108_path: Path = Path("data/inputs/tp108_stats.csv"),
    ari_threshold: float = 5.0,
) -> pd.DataFrame:
    """
    Process all catchment radar files and calculate ARI values.
    
    Args:
        radar_dir: Directory containing radar CSV files
        output_dir: Directory for ARI output files
        tp108_path: Path to TP108 coefficients CSV
        ari_threshold: Minimum ARI value to record
        
    Returns:
        Summary DataFrame with peak ARI per catchment
        
    Raises:
        FileNotFoundError: If radar_dir doesn't exist
        CoefficientsNotFoundError: If TP108 file not found
        
    Example:
        >>> summary_df = process_all_catchments(
        ...     radar_dir=Path("outputs/rain_radar/raw/radar_data"),
        ...     ari_threshold=5.0
        ... )
        >>> print(summary_df[["catchment_name", "peak_ari"]].head())
    """
    logger = logging.getLogger(__name__)
    
    if not radar_dir.exists():
        raise FileNotFoundError(
            f"Radar data directory not found: {radar_dir}\n\n"
            f"Run data collection first:\n"
            f"  python retrieve_rain_radar.py"
        )
    
    calc = ARICalculator(tp108_path=tp108_path, ari_threshold=ari_threshold)
    
    radar_files = list(radar_dir.glob("*.csv"))
    logger.info(f"Found {len(radar_files)} radar data files")
    
    if len(radar_files) == 0:
        logger.warning(f"No CSV files found in {radar_dir}")
        return pd.DataFrame()
    
    summaries = []
    
    for idx, radar_file in enumerate(radar_files, start=1):
        logger.info(f"[{idx}/{len(radar_files)}] {radar_file.name}")
        
        try:
            # Extract catchment info from filename (e.g., "123_catchment_name.csv")
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
            
        except Exception as e:
            logger.error(f"  Failed to process {radar_file.name}: {e}")
            continue
    
    summary_df = pd.DataFrame(summaries)
    
    # Save summary
    if not summary_df.empty:
        summary_path = output_dir / "ari_summary.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_df.to_csv(summary_path, index=False)
        logger.info(f"✓ Saved ARI summary to {summary_path}")
    
    return summary_df