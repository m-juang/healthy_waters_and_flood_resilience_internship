#!/usr/bin/env python3
"""
Cross-Validation Tool: Rain Gauge vs Radar Data

This script compares rainfall data from a rain gauge point measurement with
spatially aggregated radar (QPE) data from nearby pixels. It provides statistical
metrics to assess the agreement between the two data sources.

Statistical Methods Used:
    - Pearson Correlation Coefficient (r)
    - Root Mean Square Error (RMSE)
    - Mean Absolute Error (MAE)
    - Bias (Mean Error)
    - Nash-Sutcliffe Efficiency (NSE)
    - Coefficient of Determination (R²)

Usage:
    python cross_validate_gauge_radar.py --gauge-id 11315277 --date 2026-01-21
    python cross_validate_gauge_radar.py --gauge-id 11315277 --date 2026-01-21 --radius 1000
    python cross_validate_gauge_radar.py --gauge-id 11315277 --date 2026-01-21 --output results.csv

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-02-01
Version: 1.0.0
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Try to import spatial libraries
try:
    from shapely.geometry import Point
    from pyproj import Transformer
    SPATIAL_AVAILABLE = True
except ImportError:
    SPATIAL_AVAILABLE = False

# Project imports
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.common.spatial_utils import create_pixel_geometry_from_index
from moata_pipeline.logging_setup import setup_logging


# ============================================================================
# CONSTANTS
# ============================================================================

# Auckland Council Radar Grid Specifications (from Sam)
GRID_WIDTH = 512
GRID_HEIGHT = 512
PIXEL_SIZE = 500.0  # meters
ORIGIN_X = 1626250.0  # NZTM Easting (top-left centroid)
ORIGIN_Y = 6108750.0  # NZTM Northing (top-left centroid)

# Default search radius in meters
DEFAULT_RADIUS_METERS = 1000.0  # 1km radius (covers 2x2 pixels at minimum)

# Known gauge locations (estimated from location names)
# Format: gauge_id -> (lon, lat) in WGS84
KNOWN_GAUGE_LOCATIONS = {
    # West Auckland
    3160950: (174.58, -36.87),   # Swanson @ Waitakere Filter Station
    3160946: (174.63, -36.88),   # Te Pai Park Henderson
    3160982: (174.68, -36.91),   # Cutler Park New Lynn
    3160962: (174.62, -36.90),   # Opanuku @ Candia Road
    3160961: (174.58, -36.92),   # Oratia Cemetery
    3160973: (174.55, -36.78),   # Kumeu @ Waitakere Domain
    3160974: (174.53, -36.77),   # Kumeu @ Maddrens
    3160935: (174.55, -36.93),   # Waiatarua Rainfall
    3160980: (174.54, -36.94),   # Forrest Hill Road Waiatarua
    3160979: (174.55, -36.90),   # Harmel Road Waitakere
    3160984: (174.56, -36.88),   # Constable Ln Waitakere
    9601912: (174.47, -36.95),   # Piha Rangers Shed
    11271877: (174.46, -36.96),  # Piha Wetlands
    7828282: (174.51, -36.96),   # Nihotupu @ Arataki
    7828283: (174.52, -37.00),   # Waituna @ Huia Filter Station
    
    # Central Auckland
    3160995: (174.77, -36.85),   # Albert Park
    3160966: (174.72, -36.89),   # Mt Albert Grammar
    3160963: (174.79, -36.86),   # Okahu Bay Bowling Club
    3160991: (174.70, -36.90),   # Avondale Racecourse
    3160937: (174.74, -36.91),   # Whau @ Mt Roskill
    3160983: (174.75, -36.86),   # Cox's Bay Park
    3160971: (174.69, -36.88),   # Longford Park
    
    # North Shore
    11315277: (174.77, -36.79),  # Takapuna Rain @ Library
    3160996: (174.72, -36.73),   # Albany @ Hts Rd
    3160953: (174.75, -36.77),   # School @ Mairangi Bay
    3160945: (174.76, -36.70),   # Torbay @ Glamorgan School
    3160986: (174.71, -36.80),   # Birkdale (Inwards Res.)
    3160987: (174.79, -36.82),   # Bayswater @ Plymouth Res.
    3160943: (174.72, -36.78),   # Wairau at Testing Station
    3160936: (174.63, -36.79),   # Whenuapai @ Airbase
    3160960: (174.73, -36.74),   # Oteha @ Rosedale Ponds
    35643683: (174.71, -36.72),  # Oteha @ WWTP
    9599441: (174.73, -36.62),   # Whangaparaoa Rainfall
    3161018: (174.69, -36.59),   # Orewa @ Treatment Ponds
    
    # South Auckland
    9599440: (174.79, -36.97),   # Mangere Rainfall
    34966199: (174.80, -36.96),  # Mangere @ Greenwood Road
    7828285: (174.86, -36.99),   # Manukau @ Sports Bowl
    11271879: (174.91, -36.98),  # Puhinui @ Botanical Gardens
    33236625: (174.88, -36.91),  # Pakuranga @ College
    3160958: (174.94, -37.07),   # Papakura Rain @ Kaipara Rd
    7828287: (174.97, -37.10),   # Drury @ Turner Road
    3160976: (174.95, -37.03),   # Karaka rainfall
    11271880: (174.96, -37.06),  # Karaka @ Walters Road
    3160985: (175.03, -36.98),   # Clevedon @ showgrounds
    7828286: (175.05, -37.01),   # Clevedon Coast RAWS
    11271878: (174.96, -36.92),  # Mangemangeroa @ Reserve
    
    # Rural/Regional
    3160978: (174.52, -36.49),   # Hoteo at Oldfields
    3160970: (174.59, -36.47),   # Mahurangi @ Satellite Dish
    3161035: (174.65, -36.43),   # Mahurangi @ Warkworth STP
    3160972: (174.75, -36.27),   # Leigh Rainfall
    3160949: (174.68, -36.38),   # Tamahunga @ Quintals Rd
    3160955: (174.60, -36.62),   # Rangitopuni @ Walkers
    3160969: (174.52, -36.54),   # Makarau @ Folded Hills Farm
    3160977: (174.29, -36.41),   # Kaipara Heads @ Wallers
    3160965: (174.44, -36.84),   # Muriwai @ golf course
    3160990: (174.67, -36.68),   # Awanohi Rainfall @ Okura
    3160947: (174.71, -36.53),   # Te Muri Rainfall
    
    # Other
    3160942: (175.05, -37.10),   # Wairoa @ Hunua Nursery
    3160944: (175.28, -37.05),   # Waihihi @ Waharau Regional Park
    3160994: (174.78, -36.88),   # Alexandra Park Rway
    7828280: (174.82, -36.90),   # Churchill Park Rainfall
    3160951: (174.79, -36.89),   # Substation @ Lincoln Park Ave
    3160989: (174.76, -37.14),   # Awhitu @ Brook Road
    3160939: (174.74, -37.25),   # Waiuku Rain @ Waiuku-Otaua Rd
    3160964: (174.90, -37.08),   # Ngakoroa @ Donovans
    3160967: (175.03, -36.80),   # Matiatia Bay rainfall (Waiheke)
    3160975: (174.71, -36.93),   # Keeling Road @ Utilitech
    3160941: (175.29, -37.03),   # Waitangi @ Diver Road
    3160938: (175.22, -37.11),   # Whangamaire @ Culvert
    3160954: (174.68, -36.91),   # Reservoir Bush Road
    11271876: (174.76, -36.33),  # Tomarata @ Briens Farm
    3758511: (174.76, -36.33),   # Omaha Rain @ Golf Course
    3758512: (175.40, -36.18),   # RAWS @ GBI
    11271881: (174.85, -37.11),  # Mauku RAWS @ Mauku
    21526520: (174.72, -37.15),  # Clarks Beach @ Golf Course
}


# ============================================================================
# COORDINATE UTILITIES
# ============================================================================

def wgs84_to_nztm(lon: float, lat: float) -> Tuple[float, float]:
    """
    Convert WGS84 coordinates to NZTM.
    
    Args:
        lon: Longitude (WGS84)
        lat: Latitude (WGS84)
        
    Returns:
        Tuple of (easting, northing) in NZTM
    """
    if not SPATIAL_AVAILABLE:
        raise ImportError("pyproj is required for coordinate transformation")
    
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2193", always_xy=True)
    easting, northing = transformer.transform(lon, lat)
    return easting, northing


def nztm_to_pixel_index(easting: float, northing: float) -> int:
    """
    Convert NZTM coordinates to pixel index.
    
    Args:
        easting: NZTM Easting
        northing: NZTM Northing
        
    Returns:
        Pixel index (0 to 262,143)
    """
    # Calculate column and row from NZTM coordinates
    col = int((easting - ORIGIN_X + PIXEL_SIZE / 2) // PIXEL_SIZE)
    row = int((ORIGIN_Y - northing + PIXEL_SIZE / 2) // PIXEL_SIZE)
    
    # Validate bounds
    if not (0 <= col < GRID_WIDTH and 0 <= row < GRID_HEIGHT):
        raise ValueError(
            f"Coordinates ({easting}, {northing}) are outside radar grid bounds"
        )
    
    # Calculate pixel index (row-major ordering)
    pixel_index = row * GRID_WIDTH + col
    return pixel_index


def pixel_index_to_nztm(pixel_index: int) -> Tuple[float, float]:
    """
    Convert pixel index to NZTM centroid coordinates.
    
    Args:
        pixel_index: Pixel index (0 to 262,143)
        
    Returns:
        Tuple of (easting, northing) in NZTM
    """
    row = pixel_index // GRID_WIDTH
    col = pixel_index % GRID_WIDTH
    
    centroid_x = ORIGIN_X + (col * PIXEL_SIZE)
    centroid_y = ORIGIN_Y - (row * PIXEL_SIZE)
    
    return centroid_x, centroid_y


def find_nearby_pixels(
    center_easting: float,
    center_northing: float,
    radius_meters: float
) -> List[int]:
    """
    Find all pixel indices within a given radius from a center point.
    
    Args:
        center_easting: NZTM Easting of center point
        center_northing: NZTM Northing of center point
        radius_meters: Search radius in meters
        
    Returns:
        List of pixel indices within the radius
    """
    nearby_pixels = []
    
    # Calculate bounding box for search (in pixels)
    pixels_radius = int(np.ceil(radius_meters / PIXEL_SIZE)) + 1
    
    # Get center pixel
    center_col = int((center_easting - ORIGIN_X + PIXEL_SIZE / 2) // PIXEL_SIZE)
    center_row = int((ORIGIN_Y - center_northing + PIXEL_SIZE / 2) // PIXEL_SIZE)
    
    # Search in bounding box
    for row in range(max(0, center_row - pixels_radius), 
                     min(GRID_HEIGHT, center_row + pixels_radius + 1)):
        for col in range(max(0, center_col - pixels_radius),
                        min(GRID_WIDTH, center_col + pixels_radius + 1)):
            
            # Calculate pixel centroid
            pixel_x = ORIGIN_X + (col * PIXEL_SIZE)
            pixel_y = ORIGIN_Y - (row * PIXEL_SIZE)
            
            # Calculate distance from center
            distance = np.sqrt(
                (pixel_x - center_easting) ** 2 + 
                (pixel_y - center_northing) ** 2
            )
            
            # Add if within radius
            if distance <= radius_meters:
                pixel_index = row * GRID_WIDTH + col
                nearby_pixels.append(pixel_index)
    
    return nearby_pixels


# ============================================================================
# DATA LOADING
# ============================================================================

def load_gauge_data(
    gauge_id: int,
    date_str: str,
    output_base: Path = Path("outputs/rain_gauges")
) -> Tuple[Optional[Dict[str, Any]], Optional[pd.DataFrame]]:
    """
    Load rain gauge data for a specific date.
    
    Args:
        gauge_id: Rain gauge asset ID
        date_str: Date string (YYYY-MM-DD)
        output_base: Base output directory
        
    Returns:
        Tuple of (gauge_info, trace_data_df) or (None, None) if not found
    """
    logger = logging.getLogger(__name__)
    
    # Parse date and find folder
    date = datetime.strptime(date_str, "%Y-%m-%d")
    next_date = date + timedelta(days=1)
    folder_name = f"{date.strftime('%Y%m%d')}-{next_date.strftime('%Y%m%d')}"
    
    # Look for raw data file
    raw_file = output_base / folder_name / "raw" / "rain_gauges_traces_alarms.json"
    
    if not raw_file.exists():
        logger.error(f"Raw gauge data not found: {raw_file}")
        return None, None
    
    logger.info(f"Loading gauge data from: {raw_file}")
    
    with open(raw_file, 'r') as f:
        all_gauges = json.load(f)
    
    # Find the specific gauge
    for gauge_data in all_gauges:
        if gauge_data.get("gauge", {}).get("id") == gauge_id:
            gauge_info = gauge_data.get("gauge", {})
            
            # Find rainfall trace - prefer raw "Rain" trace for fair comparison
            # Priority: 1) "Rain" (raw 5-min), 2) "Raw Rain", 3) any rainfall trace
            traces = gauge_data.get("traces", [])
            rainfall_trace = None
            fallback_trace = None
            
            for trace in traces:
                dvt = trace.get("dataVariableType", {})
                dvt_name = dvt.get("name", "")
                items = trace.get("data", {}).get("items", [])
                
                if not items:
                    continue
                
                name_lower = dvt_name.lower()
                
                # Priority 1: Exact "Rain" trace (raw 5-min data)
                if dvt_name == "Rain" or dvt_name == "Anomaly Filtered Rain":
                    rainfall_trace = trace
                    logger.info(f"  Using trace: {dvt_name} ({len(items)} records)")
                    break
                
                # Priority 2: "Raw Rain"
                elif "raw rain" in name_lower and rainfall_trace is None:
                    rainfall_trace = trace
                    logger.info(f"  Using trace: {dvt_name} ({len(items)} records)")
                
                # Fallback: any rain trace that's not accumulated
                elif "rain" in name_lower and "virtual" not in name_lower and "hr" not in name_lower:
                    if fallback_trace is None:
                        fallback_trace = trace
            
            # Use fallback if no primary trace found
            if rainfall_trace is None:
                rainfall_trace = fallback_trace
                if rainfall_trace:
                    dvt_name = rainfall_trace.get("dataVariableType", {}).get("name", "")
                    items = rainfall_trace.get("data", {}).get("items", [])
                    logger.info(f"  Using fallback trace: {dvt_name} ({len(items)} records)")
            
            if rainfall_trace is None:
                logger.warning(f"No rainfall trace data found for gauge {gauge_id}")
                return gauge_info, None
            
            # Convert trace data to DataFrame
            data_items = rainfall_trace.get("data", {}).get("items", [])
            if not data_items:
                logger.warning(f"Empty rainfall data for gauge {gauge_id}")
                return gauge_info, None
            
            df = pd.DataFrame(data_items)
            
            # Convert unix timestamp to datetime
            df["timestamp"] = pd.to_datetime(
                df["whenRecordedUnixSeconds"], unit="s", utc=True
            )
            df = df.rename(columns={"value": "gauge_value"})
            df = df[["timestamp", "gauge_value"]]
            df = df.sort_values("timestamp").reset_index(drop=True)
            
            logger.info(f"Loaded {len(df)} gauge records for {gauge_info.get('name', gauge_id)}")
            
            return gauge_info, df
    
    logger.error(f"Gauge {gauge_id} not found in data")
    return None, None


def load_radar_data_for_pixels(
    pixel_indices: List[int],
    date_str: str,
    output_base: Path = Path("outputs/rain_radar")
) -> Optional[pd.DataFrame]:
    """
    Load radar data for specific pixels from catchment files.
    
    Since radar data is organized by catchment, we need to scan all catchment
    files to find data for the requested pixels.
    
    Args:
        pixel_indices: List of pixel indices to load
        date_str: Date string (YYYY-MM-DD)
        output_base: Base output directory
        
    Returns:
        DataFrame with radar data for the pixels, or None if not found
    """
    logger = logging.getLogger(__name__)
    
    # Parse date and find folder
    date = datetime.strptime(date_str, "%Y-%m-%d")
    next_date = date + timedelta(days=1)
    folder_name = f"{date.strftime('%Y%m%d')}-{next_date.strftime('%Y%m%d')}"
    
    # Look for radar data directory
    radar_dir = output_base / folder_name / "raw" / "radar_data"
    
    if not radar_dir.exists():
        logger.error(f"Radar data directory not found: {radar_dir}")
        return None
    
    logger.info(f"Searching for pixels {pixel_indices} in radar data...")
    
    pixel_set = set(pixel_indices)
    all_data = []
    
    # Scan all catchment CSV files
    for csv_file in radar_dir.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file)
            
            # Filter to requested pixels
            if "pixel_index" in df.columns:
                pixel_data = df[df["pixel_index"].isin(pixel_set)]
                if not pixel_data.empty:
                    all_data.append(pixel_data)
        except Exception as e:
            logger.warning(f"Error reading {csv_file}: {e}")
            continue
    
    if not all_data:
        logger.warning(f"No radar data found for pixels {pixel_indices}")
        return None
    
    # Combine all data
    combined = pd.concat(all_data, ignore_index=True)
    
    # Parse timestamp
    combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True)
    
    # Remove duplicates (same pixel, same timestamp)
    combined = combined.drop_duplicates(subset=["timestamp", "pixel_index"])
    
    logger.info(f"Loaded {len(combined)} radar records for {len(pixel_set)} pixels")
    
    return combined


def aggregate_radar_by_timestamp(
    radar_df: pd.DataFrame,
    method: str = "mean"
) -> pd.DataFrame:
    """
    Aggregate radar data across pixels to a single value per timestamp.
    
    Args:
        radar_df: DataFrame with columns [timestamp, pixel_index, value, weight, weighted_value]
        method: Aggregation method ("mean", "weighted_mean", "max", "median")
        
    Returns:
        DataFrame with columns [timestamp, radar_value]
    """
    if radar_df is None or radar_df.empty:
        return pd.DataFrame(columns=["timestamp", "radar_value"])
    
    if method == "weighted_mean":
        # Use pre-computed weighted values if available
        if "weighted_value" in radar_df.columns and "weight" in radar_df.columns:
            agg = radar_df.groupby("timestamp").agg({
                "weighted_value": "sum",
                "weight": "sum"
            }).reset_index()
            agg["radar_value"] = agg["weighted_value"] / agg["weight"].clip(lower=1e-10)
            return agg[["timestamp", "radar_value"]]
    
    # Simple aggregation
    if method == "mean":
        agg = radar_df.groupby("timestamp")["value"].mean().reset_index()
    elif method == "max":
        agg = radar_df.groupby("timestamp")["value"].max().reset_index()
    elif method == "median":
        agg = radar_df.groupby("timestamp")["value"].median().reset_index()
    else:
        agg = radar_df.groupby("timestamp")["value"].mean().reset_index()
    
    agg = agg.rename(columns={"value": "radar_value"})
    return agg


# ============================================================================
# STATISTICAL METRICS
# ============================================================================

def calculate_validation_metrics(
    gauge: np.ndarray,
    radar: np.ndarray
) -> Dict[str, float]:
    """
    Calculate comprehensive validation metrics between gauge and radar data.
    
    Args:
        gauge: Array of gauge rainfall values
        radar: Array of radar rainfall values (same length as gauge)
        
    Returns:
        Dictionary of statistical metrics
    """
    # Remove NaN values
    mask = ~(np.isnan(gauge) | np.isnan(radar))
    gauge_clean = gauge[mask]
    radar_clean = radar[mask]
    
    n = len(gauge_clean)
    
    if n < 2:
        return {
            "n_samples": n,
            "correlation": np.nan,
            "rmse": np.nan,
            "mae": np.nan,
            "bias": np.nan,
            "nse": np.nan,
            "r_squared": np.nan,
            "percent_bias": np.nan,
            "gauge_mean": np.nan,
            "radar_mean": np.nan,
            "gauge_std": np.nan,
            "radar_std": np.nan,
        }
    
    # Basic statistics
    gauge_mean = np.mean(gauge_clean)
    radar_mean = np.mean(radar_clean)
    gauge_std = np.std(gauge_clean)
    radar_std = np.std(radar_clean)
    
    # Pearson Correlation Coefficient
    if gauge_std > 0 and radar_std > 0:
        correlation = np.corrcoef(gauge_clean, radar_clean)[0, 1]
    else:
        correlation = np.nan
    
    # Error metrics
    errors = radar_clean - gauge_clean
    
    # Bias (Mean Error) - positive = radar overestimates
    bias = np.mean(errors)
    
    # Percent Bias
    if gauge_mean != 0:
        percent_bias = 100 * (radar_mean - gauge_mean) / gauge_mean
    else:
        percent_bias = np.nan
    
    # MAE (Mean Absolute Error)
    mae = np.mean(np.abs(errors))
    
    # RMSE (Root Mean Square Error)
    rmse = np.sqrt(np.mean(errors ** 2))
    
    # R² (Coefficient of Determination)
    if not np.isnan(correlation):
        r_squared = correlation ** 2
    else:
        r_squared = np.nan
    
    # NSE (Nash-Sutcliffe Efficiency)
    # NSE = 1 - [Σ(obs - sim)² / Σ(obs - obs_mean)²]
    # NSE = 1 is perfect, NSE < 0 means model is worse than using mean
    ss_res = np.sum(errors ** 2)
    ss_tot = np.sum((gauge_clean - gauge_mean) ** 2)
    
    if ss_tot > 0:
        nse = 1 - (ss_res / ss_tot)
    else:
        nse = np.nan
    
    return {
        "n_samples": n,
        "correlation": round(correlation, 4) if not np.isnan(correlation) else np.nan,
        "rmse": round(rmse, 4),
        "mae": round(mae, 4),
        "bias": round(bias, 4),
        "nse": round(nse, 4) if not np.isnan(nse) else np.nan,
        "r_squared": round(r_squared, 4) if not np.isnan(r_squared) else np.nan,
        "percent_bias": round(percent_bias, 2) if not np.isnan(percent_bias) else np.nan,
        "gauge_mean": round(gauge_mean, 4),
        "radar_mean": round(radar_mean, 4),
        "gauge_std": round(gauge_std, 4),
        "radar_std": round(radar_std, 4),
    }


def print_metrics_report(
    metrics: Dict[str, float],
    gauge_name: str,
    n_pixels: int,
    radius: float
) -> None:
    """Print a formatted metrics report."""
    
    print("\n" + "=" * 70)
    print("CROSS-VALIDATION REPORT: Rain Gauge vs Radar")
    print("=" * 70)
    print(f"\nGauge: {gauge_name}")
    print(f"Search Radius: {radius:.0f} meters")
    print(f"Nearby Pixels: {n_pixels}")
    print(f"Matched Samples: {metrics['n_samples']}")
    
    print("\n" + "-" * 40)
    print("STATISTICAL METRICS")
    print("-" * 40)
    
    # Correlation
    corr = metrics.get("correlation", np.nan)
    if not np.isnan(corr):
        corr_quality = "Excellent" if corr > 0.9 else "Good" if corr > 0.7 else "Moderate" if corr > 0.5 else "Poor"
        print(f"Pearson Correlation (r):     {corr:>8.4f}  [{corr_quality}]")
    else:
        print(f"Pearson Correlation (r):     {'N/A':>8}")
    
    # R²
    r2 = metrics.get("r_squared", np.nan)
    if not np.isnan(r2):
        print(f"R² (Coefficient of Det.):    {r2:>8.4f}  [{r2*100:.1f}% variance explained]")
    
    # NSE
    nse = metrics.get("nse", np.nan)
    if not np.isnan(nse):
        nse_quality = "Excellent" if nse > 0.75 else "Good" if nse > 0.50 else "Acceptable" if nse > 0.0 else "Poor"
        print(f"Nash-Sutcliffe Efficiency:   {nse:>8.4f}  [{nse_quality}]")
    
    print("\n" + "-" * 40)
    print("ERROR METRICS")
    print("-" * 40)
    
    print(f"RMSE (mm):                   {metrics.get('rmse', np.nan):>8.4f}")
    print(f"MAE (mm):                    {metrics.get('mae', np.nan):>8.4f}")
    print(f"Bias (mm):                   {metrics.get('bias', np.nan):>8.4f}", end="")
    
    bias = metrics.get("bias", 0)
    if bias > 0:
        print("  [Radar overestimates]")
    elif bias < 0:
        print("  [Radar underestimates]")
    else:
        print()
    
    pb = metrics.get("percent_bias", np.nan)
    if not np.isnan(pb):
        print(f"Percent Bias (%):            {pb:>8.2f}")
    
    print("\n" + "-" * 40)
    print("SUMMARY STATISTICS")
    print("-" * 40)
    print(f"Gauge Mean (mm):             {metrics.get('gauge_mean', np.nan):>8.4f}")
    print(f"Radar Mean (mm):             {metrics.get('radar_mean', np.nan):>8.4f}")
    print(f"Gauge Std Dev (mm):          {metrics.get('gauge_std', np.nan):>8.4f}")
    print(f"Radar Std Dev (mm):          {metrics.get('radar_std', np.nan):>8.4f}")
    
    print("\n" + "=" * 70)


# ============================================================================
# HTML DASHBOARD GENERATION
# ============================================================================

def get_quality_color(value: float, metric_type: str) -> str:
    """Get color based on metric quality."""
    if np.isnan(value):
        return "#6c757d"  # Gray for N/A
    
    if metric_type == "correlation":
        if value > 0.9:
            return "#28a745"  # Green
        elif value > 0.7:
            return "#20c997"  # Teal
        elif value > 0.5:
            return "#ffc107"  # Yellow
        else:
            return "#dc3545"  # Red
    
    elif metric_type == "nse":
        if value > 0.75:
            return "#28a745"
        elif value > 0.50:
            return "#20c997"
        elif value > 0.0:
            return "#ffc107"
        else:
            return "#dc3545"
    
    elif metric_type == "r_squared":
        if value > 0.8:
            return "#28a745"
        elif value > 0.5:
            return "#20c997"
        elif value > 0.25:
            return "#ffc107"
        else:
            return "#dc3545"
    
    return "#17a2b8"  # Default info blue


def get_quality_label(value: float, metric_type: str) -> str:
    """Get quality label based on metric value."""
    if np.isnan(value):
        return "N/A"
    
    if metric_type == "correlation":
        if value > 0.9:
            return "Excellent"
        elif value > 0.7:
            return "Good"
        elif value > 0.5:
            return "Moderate"
        else:
            return "Poor"
    
    elif metric_type == "nse":
        if value > 0.75:
            return "Excellent"
        elif value > 0.50:
            return "Good"
        elif value > 0.0:
            return "Acceptable"
        else:
            return "Poor"
    
    elif metric_type == "r_squared":
        if value > 0.8:
            return "Excellent"
        elif value > 0.5:
            return "Good"
        elif value > 0.25:
            return "Moderate"
        else:
            return "Poor"
    
    return ""


def generate_html_dashboard(
    result: Dict[str, Any],
    merged_df: pd.DataFrame,
    output_path: Path
) -> Path:
    """
    Generate an interactive HTML dashboard for cross-validation results.
    
    Args:
        result: Dictionary with validation results
        merged_df: DataFrame with merged gauge and radar data
        output_path: Path to save HTML file
        
    Returns:
        Path to generated HTML file
    """
    metrics = result["metrics"]
    gauge_name = result["gauge_name"]
    gauge_id = result["gauge_id"]
    date_str = result["date"]
    radius = result["radius_meters"]
    n_pixels = result["n_nearby_pixels"]
    aggregation = result["aggregation_method"]
    coords_wgs84 = result["gauge_coords_wgs84"]
    coords_nztm = result["gauge_coords_nztm"]
    
    # Prepare time series data for chart
    merged_reset = merged_df.reset_index()
    timestamps = merged_reset["timestamp"].dt.strftime("%Y-%m-%d %H:%M").tolist()
    gauge_values = merged_reset["gauge_value"].round(4).tolist()
    radar_values = merged_reset["radar_value"].round(4).tolist()
    
    # Calculate additional stats for scatter plot
    gauge_arr = np.array(gauge_values)
    radar_arr = np.array(radar_values)
    
    # Get quality indicators
    corr = metrics.get("correlation", np.nan)
    nse = metrics.get("nse", np.nan)
    r2 = metrics.get("r_squared", np.nan)
    
    corr_color = get_quality_color(corr, "correlation")
    nse_color = get_quality_color(nse, "nse")
    r2_color = get_quality_color(r2, "r_squared")
    
    corr_label = get_quality_label(corr, "correlation")
    nse_label = get_quality_label(nse, "nse")
    r2_label = get_quality_label(r2, "r_squared")
    
    # Bias interpretation
    bias = metrics.get("bias", 0)
    if bias > 0:
        bias_interpretation = "Radar overestimates rainfall"
        bias_icon = "↑"
    elif bias < 0:
        bias_interpretation = "Radar underestimates rainfall"
        bias_icon = "↓"
    else:
        bias_interpretation = "No systematic bias"
        bias_icon = "="
    
    # Pre-format metric values for HTML (to avoid ternary in f-strings)
    corr_display = f"{corr:.4f}" if not np.isnan(corr) else "N/A"
    r2_display = f"{r2:.4f}" if not np.isnan(r2) else "N/A"
    r2_pct_display = f"{r2*100:.1f}" if not np.isnan(r2) else "0.0"
    nse_display = f"{nse:.4f}" if not np.isnan(nse) else "N/A"
    bias_abs_display = f"{abs(bias):.4f}"
    percent_bias = metrics.get('percent_bias', 0)
    percent_bias_display = f"{percent_bias:.1f}" if not np.isnan(percent_bias) else "0.0"
    
    # Bias colors
    bias_value_color = '#dc3545' if abs(bias) > 0.5 else '#ffc107' if abs(bias) > 0.1 else '#28a745'
    bias_badge_color = '#dc3545' if abs(percent_bias) > 50 else '#ffc107' if abs(percent_bias) > 20 else '#28a745'
    
    # Error metric displays
    rmse_display = f"{metrics.get('rmse', 0):.4f}"
    mae_display = f"{metrics.get('mae', 0):.4f}"
    gauge_mean_display = f"{metrics.get('gauge_mean', 0):.4f}"
    radar_mean_display = f"{metrics.get('radar_mean', 0):.4f}"
    
    # Interpretation text
    corr_interp = 'Strong linear relationship between gauge and radar measurements.' if corr > 0.7 else 'Moderate agreement between data sources.' if corr > 0.5 else 'Weak correlation suggests significant differences in measurements.'
    nse_interp = 'Radar data is a good predictor of gauge measurements.' if nse > 0.5 else 'Radar provides some predictive value.' if nse > 0 else 'Using the mean gauge value would be more accurate than radar predictions.'
    bias_interp_suffix = 'This is within acceptable limits.' if abs(percent_bias) < 25 else 'Consider calibration adjustment.'
    
    # Bullet classes
    corr_bullet = 'success' if corr > 0.7 else 'warning' if corr > 0.5 else 'danger'
    nse_bullet = 'success' if nse > 0.5 else 'warning' if nse > 0 else 'danger'
    
    # Max value for scatter plot
    max_gauge = max(gauge_values) if gauge_values else 1
    
    # Generate HTML
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cross-Validation Report: {gauge_name}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
    <style>
        :root {{
            --primary: #0d6efd;
            --success: #28a745;
            --warning: #ffc107;
            --danger: #dc3545;
            --info: #17a2b8;
            --dark: #343a40;
            --light: #f8f9fa;
            --gray: #6c757d;
        }}
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        .header {{
            background: white;
            border-radius: 16px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            color: var(--dark);
            font-size: 1.8rem;
            margin-bottom: 10px;
        }}
        
        .header .subtitle {{
            color: var(--gray);
            font-size: 1rem;
        }}
        
        .header .meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid #eee;
        }}
        
        .meta-item {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .meta-item .icon {{
            font-size: 1.2rem;
        }}
        
        .meta-item .label {{
            color: var(--gray);
            font-size: 0.85rem;
        }}
        
        .meta-item .value {{
            font-weight: 600;
            color: var(--dark);
        }}
        
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        
        .card {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }}
        
        .card-title {{
            font-size: 0.9rem;
            color: var(--gray);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 15px;
        }}
        
        .metric-large {{
            font-size: 3rem;
            font-weight: 700;
            line-height: 1;
        }}
        
        .metric-label {{
            font-size: 0.9rem;
            color: var(--gray);
            margin-top: 8px;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            color: white;
            margin-top: 10px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }}
        
        .stat-item {{
            padding: 15px;
            background: var(--light);
            border-radius: 10px;
        }}
        
        .stat-item .value {{
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--dark);
        }}
        
        .stat-item .label {{
            font-size: 0.8rem;
            color: var(--gray);
            margin-top: 4px;
        }}
        
        .chart-container {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 20px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }}
        
        .chart-title {{
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--dark);
            margin-bottom: 20px;
        }}
        
        .chart-wrapper {{
            position: relative;
            height: 350px;
        }}
        
        .interpretation {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 20px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }}
        
        .interpretation h3 {{
            color: var(--dark);
            margin-bottom: 15px;
        }}
        
        .interpretation ul {{
            list-style: none;
            padding: 0;
        }}
        
        .interpretation li {{
            padding: 12px 0;
            border-bottom: 1px solid #eee;
            display: flex;
            align-items: flex-start;
            gap: 12px;
        }}
        
        .interpretation li:last-child {{
            border-bottom: none;
        }}
        
        .interpretation .bullet {{
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.8rem;
            flex-shrink: 0;
        }}
        
        .bullet.success {{ background: #d4edda; color: #155724; }}
        .bullet.warning {{ background: #fff3cd; color: #856404; }}
        .bullet.danger {{ background: #f8d7da; color: #721c24; }}
        .bullet.info {{ background: #d1ecf1; color: #0c5460; }}
        
        .footer {{
            text-align: center;
            color: rgba(255,255,255,0.8);
            padding: 20px;
            font-size: 0.85rem;
        }}
        
        .two-col {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        
        @media (max-width: 768px) {{
            .two-col {{
                grid-template-columns: 1fr;
            }}
            .header .meta {{
                flex-direction: column;
                gap: 10px;
            }}
        }}
        
        .methodology {{
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 20px;
        }}
        
        .methodology h3 {{
            color: var(--dark);
            margin-bottom: 15px;
        }}
        
        .methodology-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
        }}
        
        .methodology-item {{
            background: white;
            padding: 15px;
            border-radius: 10px;
        }}
        
        .methodology-item h4 {{
            color: var(--primary);
            font-size: 0.9rem;
            margin-bottom: 8px;
        }}
        
        .methodology-item p {{
            color: var(--gray);
            font-size: 0.85rem;
            line-height: 1.5;
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <h1>🌧️ Cross-Validation Report: Rain Gauge vs Radar</h1>
            <p class="subtitle">Comparing point-based gauge measurements with spatial radar (QPE) data</p>
            
            <div class="meta">
                <div class="meta-item">
                    <span class="icon">📍</span>
                    <div>
                        <div class="label">Rain Gauge</div>
                        <div class="value">{gauge_name}</div>
                    </div>
                </div>
                <div class="meta-item">
                    <span class="icon">📅</span>
                    <div>
                        <div class="label">Date</div>
                        <div class="value">{date_str}</div>
                    </div>
                </div>
                <div class="meta-item">
                    <span class="icon">📡</span>
                    <div>
                        <div class="label">Nearby Pixels</div>
                        <div class="value">{n_pixels} pixels within {radius:.0f}m</div>
                    </div>
                </div>
                <div class="meta-item">
                    <span class="icon">📊</span>
                    <div>
                        <div class="label">Aggregation</div>
                        <div class="value">{aggregation.title()}</div>
                    </div>
                </div>
                <div class="meta-item">
                    <span class="icon">🔢</span>
                    <div>
                        <div class="label">Matched Samples</div>
                        <div class="value">{metrics['n_samples']}</div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Key Metrics -->
        <div class="grid">
            <div class="card">
                <div class="card-title">Pearson Correlation (r)</div>
                <div class="metric-large" style="color: {corr_color}">
                    {corr_display}
                </div>
                <div class="metric-label">Linear relationship strength (-1 to 1)</div>
                <span class="badge" style="background: {corr_color}">{corr_label}</span>
            </div>
            
            <div class="card">
                <div class="card-title">R² (Coefficient of Determination)</div>
                <div class="metric-large" style="color: {r2_color}">
                    {r2_display}
                </div>
                <div class="metric-label">{r2_pct_display}% of variance explained</div>
                <span class="badge" style="background: {r2_color}">{r2_label}</span>
            </div>
            
            <div class="card">
                <div class="card-title">Nash-Sutcliffe Efficiency</div>
                <div class="metric-large" style="color: {nse_color}">
                    {nse_display}
                </div>
                <div class="metric-label">Hydrological model efficiency (-∞ to 1)</div>
                <span class="badge" style="background: {nse_color}">{nse_label}</span>
            </div>
            
            <div class="card">
                <div class="card-title">Bias Analysis</div>
                <div class="metric-large" style="color: {bias_value_color}">
                    {bias_icon} {bias_abs_display} mm
                </div>
                <div class="metric-label">{bias_interpretation}</div>
                <span class="badge" style="background: {bias_badge_color}">
                    {percent_bias_display}% bias
                </span>
            </div>
        </div>
        
        <!-- Error Metrics -->
        <div class="card" style="margin-bottom: 20px;">
            <div class="card-title">Error Metrics</div>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="value">{rmse_display} mm</div>
                    <div class="label">RMSE (Root Mean Square Error)</div>
                </div>
                <div class="stat-item">
                    <div class="value">{mae_display} mm</div>
                    <div class="label">MAE (Mean Absolute Error)</div>
                </div>
                <div class="stat-item">
                    <div class="value">{gauge_mean_display} mm</div>
                    <div class="label">Gauge Mean Rainfall</div>
                </div>
                <div class="stat-item">
                    <div class="value">{radar_mean_display} mm</div>
                    <div class="label">Radar Mean Rainfall</div>
                </div>
            </div>
        </div>
        
        <!-- Time Series Chart -->
        <div class="chart-container">
            <div class="chart-title">📈 Time Series Comparison</div>
            <div class="chart-wrapper">
                <canvas id="timeSeriesChart"></canvas>
            </div>
        </div>
        
        <!-- Scatter Plot -->
        <div class="two-col">
            <div class="chart-container">
                <div class="chart-title">🔵 Scatter Plot: Gauge vs Radar</div>
                <div class="chart-wrapper">
                    <canvas id="scatterChart"></canvas>
                </div>
            </div>
            
            <div class="interpretation">
                <h3>📋 Interpretation Guide</h3>
                <ul>
                    <li>
                        <span class="bullet {corr_bullet}">r</span>
                        <div>
                            <strong>Correlation: {corr_label}</strong><br>
                            <span style="color: var(--gray)">
                                {corr_interp}
                            </span>
                        </div>
                    </li>
                    <li>
                        <span class="bullet {nse_bullet}">N</span>
                        <div>
                            <strong>NSE: {nse_label}</strong><br>
                            <span style="color: var(--gray)">
                                {nse_interp}
                            </span>
                        </div>
                    </li>
                    <li>
                        <span class="bullet info">{bias_icon}</span>
                        <div>
                            <strong>Bias: {percent_bias_display}%</strong><br>
                            <span style="color: var(--gray)">{bias_interpretation}. 
                                {bias_interp_suffix}
                            </span>
                        </div>
                    </li>
                    <li>
                        <span class="bullet info">⚡</span>
                        <div>
                            <strong>Data Resolution</strong><br>
                            <span style="color: var(--gray)">
                                Gauge: point measurement. Radar: {n_pixels} pixels ({n_pixels * 0.25:.2f} km² area) averaged using {aggregation} method.
                            </span>
                        </div>
                    </li>
                </ul>
            </div>
        </div>
        
        <!-- Methodology -->
        <div class="methodology">
            <h3>📚 Methodology & Metrics Explained</h3>
            <div class="methodology-grid">
                <div class="methodology-item">
                    <h4>Pearson Correlation (r)</h4>
                    <p>Measures linear relationship between two variables. Range: -1 to 1. Values > 0.7 indicate strong positive correlation.</p>
                </div>
                <div class="methodology-item">
                    <h4>R² (Coefficient of Determination)</h4>
                    <p>Proportion of variance in gauge data explained by radar. R² = r². Value of 0.64 means 64% variance explained.</p>
                </div>
                <div class="methodology-item">
                    <h4>Nash-Sutcliffe Efficiency (NSE)</h4>
                    <p>Standard metric in hydrology. NSE = 1 is perfect. NSE = 0 means radar is as good as using mean. NSE &lt; 0 means worse than mean.</p>
                </div>
                <div class="methodology-item">
                    <h4>RMSE & MAE</h4>
                    <p>Error metrics measuring average prediction error. RMSE penalizes large errors more heavily. Lower values indicate better agreement.</p>
                </div>
                <div class="methodology-item">
                    <h4>Bias</h4>
                    <p>Systematic error. Positive bias = radar overestimates. Negative bias = radar underestimates. Percent bias relative to gauge mean.</p>
                </div>
                <div class="methodology-item">
                    <h4>Radar Aggregation</h4>
                    <p>Nearby radar pixels within {radius:.0f}m radius are aggregated using {aggregation} method to compare with point gauge measurement.</p>
                </div>
            </div>
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <p>Generated by MOATA AlertLab Cross-Validation Tool | Auckland Council</p>
            <p>Report generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        </div>
    </div>
    
    <script>
        // Time Series Chart
        const tsCtx = document.getElementById('timeSeriesChart').getContext('2d');
        new Chart(tsCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(timestamps)},
                datasets: [
                    {{
                        label: 'Rain Gauge',
                        data: {json.dumps(gauge_values)},
                        borderColor: '#0d6efd',
                        backgroundColor: 'rgba(13, 110, 253, 0.1)',
                        fill: true,
                        tension: 0.1,
                        pointRadius: 2
                    }},
                    {{
                        label: 'Radar (QPE)',
                        data: {json.dumps(radar_values)},
                        borderColor: '#dc3545',
                        backgroundColor: 'rgba(220, 53, 69, 0.1)',
                        fill: true,
                        tension: 0.1,
                        pointRadius: 2
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                interaction: {{
                    mode: 'index',
                    intersect: false
                }},
                scales: {{
                    x: {{
                        title: {{ display: true, text: 'Time' }},
                        ticks: {{ maxTicksLimit: 12 }}
                    }},
                    y: {{
                        title: {{ display: true, text: 'Rainfall (mm)' }},
                        beginAtZero: true
                    }}
                }},
                plugins: {{
                    legend: {{ position: 'top' }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return context.dataset.label + ': ' + context.parsed.y.toFixed(4) + ' mm';
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Scatter Plot
        const scatterData = {json.dumps(gauge_values)}.map((g, i) => ({{
            x: g,
            y: {json.dumps(radar_values)}[i]
        }}));
        
        const scatterCtx = document.getElementById('scatterChart').getContext('2d');
        new Chart(scatterCtx, {{
            type: 'scatter',
            data: {{
                datasets: [
                    {{
                        label: 'Gauge vs Radar',
                        data: scatterData,
                        backgroundColor: 'rgba(13, 110, 253, 0.6)',
                        borderColor: '#0d6efd',
                        pointRadius: 5
                    }},
                    {{
                        label: '1:1 Line (Perfect Agreement)',
                        data: [{{x: 0, y: 0}}, {{x: {max_gauge}, y: {max_gauge}}}],
                        type: 'line',
                        borderColor: '#28a745',
                        borderDash: [5, 5],
                        pointRadius: 0,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    x: {{
                        title: {{ display: true, text: 'Gauge Rainfall (mm)' }},
                        beginAtZero: true
                    }},
                    y: {{
                        title: {{ display: true, text: 'Radar Rainfall (mm)' }},
                        beginAtZero: true
                    }}
                }},
                plugins: {{
                    legend: {{ position: 'top' }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                if (context.datasetIndex === 0) {{
                                    return 'Gauge: ' + context.parsed.x.toFixed(4) + ' mm, Radar: ' + context.parsed.y.toFixed(4) + ' mm';
                                }}
                                return context.dataset.label;
                            }}
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
'''
    
    # Write HTML file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return output_path


# ============================================================================
# MAIN CROSS-VALIDATION FUNCTION
# ============================================================================

def cross_validate_gauge_radar(
    gauge_id: int,
    date_str: str,
    radius_meters: float = DEFAULT_RADIUS_METERS,
    gauge_coords: Optional[Tuple[float, float]] = None,
    aggregation_method: str = "mean",
    output_file: Optional[Path] = None,
    html_output: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """
    Cross-validate rain gauge data against nearby radar pixels.
    
    Args:
        gauge_id: Rain gauge asset ID
        date_str: Date string (YYYY-MM-DD)
        radius_meters: Search radius for nearby pixels (meters)
        gauge_coords: Optional (longitude, latitude) tuple for gauge location
                     If not provided, will try to find from API or use default
        aggregation_method: How to aggregate radar pixels ("mean", "weighted_mean", "max", "median")
        output_file: Optional path to save results CSV
        html_output: Optional path to save HTML dashboard
        
    Returns:
        Dictionary with validation results, or None if failed
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting cross-validation for gauge {gauge_id} on {date_str}")
    
    # Load gauge data
    gauge_info, gauge_df = load_gauge_data(gauge_id, date_str)
    
    if gauge_info is None:
        logger.error(f"Failed to load gauge data for {gauge_id}")
        return None
    
    if gauge_df is None or gauge_df.empty:
        logger.error(f"No rainfall trace data for gauge {gauge_id}")
        return None
    
    gauge_name = gauge_info.get("name", f"Gauge {gauge_id}")
    logger.info(f"Gauge: {gauge_name}")
    
    # Get gauge coordinates
    if gauge_coords is None:
        # Try to extract from gauge info (if available)
        # Auckland Council gauges typically have coordinates in metadata
        # For now, we'll use a default location (Auckland CBD area)
        # In production, this should be fetched from API
        
        # Default: Auckland CBD area for testing
        logger.warning(
            "Gauge coordinates not provided. Using approximate location. "
            "For accurate results, provide --lon and --lat arguments."
        )
        # Use Swanson area as default (where we have test data)
        gauge_coords = (174.5833, -36.8667)  # Approximate Swanson area
    
    lon, lat = gauge_coords
    logger.info(f"Gauge location: ({lon:.4f}, {lat:.4f}) WGS84")
    
    # Convert to NZTM
    try:
        easting, northing = wgs84_to_nztm(lon, lat)
        logger.info(f"NZTM coordinates: ({easting:.1f}, {northing:.1f})")
    except Exception as e:
        logger.error(f"Failed to convert coordinates: {e}")
        return None
    
    # Find nearby pixels
    nearby_pixels = find_nearby_pixels(easting, northing, radius_meters)
    logger.info(f"Found {len(nearby_pixels)} pixels within {radius_meters:.0f}m radius")
    
    if not nearby_pixels:
        logger.error("No radar pixels found near gauge location")
        return None
    
    # Load radar data for nearby pixels
    radar_df = load_radar_data_for_pixels(nearby_pixels, date_str)
    
    if radar_df is None or radar_df.empty:
        logger.error("Failed to load radar data for nearby pixels")
        return None
    
    # Aggregate radar data by timestamp
    radar_agg = aggregate_radar_by_timestamp(radar_df, method=aggregation_method)
    logger.info(f"Aggregated radar data: {len(radar_agg)} timestamps (1-min resolution)")
    
    # Merge gauge and radar data on timestamp
    # First, determine gauge resolution
    gauge_df = gauge_df.set_index("timestamp")
    radar_agg = radar_agg.set_index("timestamp")
    
    # Detect gauge resolution (time between samples)
    gauge_sorted = gauge_df.sort_index()
    if len(gauge_sorted) >= 2:
        time_diffs = gauge_sorted.index.to_series().diff().dropna()
        median_diff = time_diffs.median()
        gauge_resolution_min = int(median_diff.total_seconds() / 60)
        logger.info(f"Detected gauge resolution: {gauge_resolution_min} minutes")
    else:
        gauge_resolution_min = 5  # Default to 5 minutes
    
    # Radar data is typically 1-minute resolution
    # We need to SUM radar values to match gauge accumulation period
    # E.g., if gauge is 5-min accumulation, sum 5 radar readings
    resample_str = f"{gauge_resolution_min}min"
    
    # CRITICAL: Gauge timestamps represent END of accumulation period
    # e.g., gauge 00:05:00 = rainfall from 00:00:00 to 00:05:00
    # So radar resample must use label='right' to match gauge convention
    # closed='left' means interval [00:00, 00:05) includes 00:00,01,02,03,04 -> labeled 00:05
    
    # Resample radar: SUM to get accumulation over the period (not mean!)
    # Use label='right', closed='left' to match gauge timestamp convention
    # [00:00, 00:05) -> labeled 00:05:00 = sum of 00:00-00:04
    radar_resampled = radar_agg.resample(resample_str, label='right', closed='left').sum()
    
    # Resample gauge to same resolution (should already be at this resolution)
    # Also use same convention for consistency
    gauge_resampled = gauge_df.resample(resample_str, label='right', closed='left').mean()
    
    logger.info(f"Resampled to {gauge_resolution_min}-min resolution: gauge={len(gauge_resampled)}, radar={len(radar_resampled)}")
    
    # Merge on common timestamps
    merged = gauge_resampled.join(radar_resampled, how="inner")
    merged = merged.dropna()
    
    logger.info(f"Matched {len(merged)} timestamps after alignment")
    
    if len(merged) < 10:
        logger.warning(f"Very few matched samples ({len(merged)}). Results may be unreliable.")
    
    if len(merged) == 0:
        logger.error("No matching timestamps between gauge and radar data")
        return None
    
    # Calculate validation metrics
    gauge_values = merged["gauge_value"].values
    radar_values = merged["radar_value"].values
    
    metrics = calculate_validation_metrics(gauge_values, radar_values)
    
    # Print report
    print_metrics_report(metrics, gauge_name, len(nearby_pixels), radius_meters)
    
    # Save merged data if output file specified
    if output_file:
        merged_reset = merged.reset_index()
        merged_reset["gauge_id"] = gauge_id
        merged_reset["gauge_name"] = gauge_name
        merged_reset["n_pixels"] = len(nearby_pixels)
        merged_reset["radius_m"] = radius_meters
        merged_reset.to_csv(output_file, index=False)
        logger.info(f"Saved merged data to: {output_file}")
    
    # Prepare result dictionary
    result = {
        "gauge_id": gauge_id,
        "gauge_name": gauge_name,
        "date": date_str,
        "gauge_coords_wgs84": gauge_coords,
        "gauge_coords_nztm": (easting, northing),
        "radius_meters": radius_meters,
        "n_nearby_pixels": len(nearby_pixels),
        "nearby_pixels": nearby_pixels,
        "n_matched_samples": len(merged),
        "metrics": metrics,
        "aggregation_method": aggregation_method,
    }
    
    # Generate HTML dashboard if specified
    if html_output:
        try:
            html_path = generate_html_dashboard(result, merged, html_output)
            logger.info(f"Generated HTML dashboard: {html_path}")
            result["html_report"] = str(html_path)
        except Exception as e:
            logger.error(f"Failed to generate HTML dashboard: {e}")
    
    # Return results
    return result


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Cross-validate rain gauge data against nearby radar pixels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic usage with gauge ID and date
    python cross_validate_gauge_radar.py --gauge-id 11315277 --date 2026-01-21
    
    # Specify gauge coordinates and search radius
    python cross_validate_gauge_radar.py --gauge-id 11315277 --date 2026-01-21 \\
        --lon 174.5833 --lat -36.8667 --radius 1500
    
    # Save results to CSV
    python cross_validate_gauge_radar.py --gauge-id 11315277 --date 2026-01-21 \\
        --output validation_results.csv
    
    # Generate HTML dashboard
    python cross_validate_gauge_radar.py --gauge-id 11315277 --date 2026-01-21 \\
        --html cross_validation_report.html

Statistical Metrics Explained:
    - Correlation (r): Pearson correlation coefficient (-1 to 1)
    - R²: Coefficient of determination (0 to 1)
    - NSE: Nash-Sutcliffe Efficiency (-∞ to 1, 1=perfect)
    - RMSE: Root Mean Square Error (lower is better)
    - MAE: Mean Absolute Error (lower is better)
    - Bias: Mean Error (positive=radar overestimates)
        """
    )
    
    parser.add_argument(
        "--gauge-id",
        type=int,
        help="Single rain gauge asset ID (e.g., 3160950)"
    )
    
    parser.add_argument(
        "--gauge-ids",
        type=str,
        help="Comma-separated list of gauge IDs (e.g., 3160950,9599440,3160966)"
    )
    
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run cross-validation for ALL gauges with known coordinates (slow)"
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available gauges with known coordinates and exit"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="Date to analyze (YYYY-MM-DD format). Required unless --list is used."
    )
    
    parser.add_argument(
        "--lon",
        type=float,
        help="Gauge longitude (WGS84, e.g., 174.5833)"
    )
    
    parser.add_argument(
        "--lat",
        type=float,
        help="Gauge latitude (WGS84, e.g., -36.8667)"
    )
    
    parser.add_argument(
        "--radius",
        type=float,
        default=DEFAULT_RADIUS_METERS,
        help=f"Search radius in meters (default: {DEFAULT_RADIUS_METERS})"
    )
    
    parser.add_argument(
        "--aggregation",
        type=str,
        choices=["mean", "weighted_mean", "max", "median"],
        default="mean",
        help="How to aggregate radar pixels (default: mean)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Output CSV file path for merged timeseries data"
    )
    
    parser.add_argument(
        "--html",
        type=str,
        help="Output HTML file path for interactive dashboard"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()


def list_available_gauges():
    """Print all available gauges with known coordinates."""
    print("\n" + "="*85)
    print("AVAILABLE GAUGES WITH KNOWN COORDINATES")
    print("="*85)
    print(f"{'ID':<12} {'Location':<55} {'Lon':>8} {'Lat':>8}")
    print("-"*85)
    
    # Group by region
    regions = {
        "West Auckland": [3160950, 3160946, 3160982, 3160962, 3160961, 3160973, 3160974, 
                          3160935, 3160980, 3160979, 3160984, 9601912, 11271877, 7828282, 7828283],
        "Central Auckland": [3160995, 3160966, 3160963, 3160991, 3160937, 3160983, 3160971],
        "North Shore": [11315277, 3160996, 3160953, 3160945, 3160986, 3160987, 3160943, 3160960],
        "South Auckland": [9599440, 34966199, 7828285, 7828287, 3160958, 3160985, 7828286, 
                           11271879, 11271880, 11271881, 33236625],
        "Rural/Outer": [3160936, 3160970, 3161035, 3160972, 3160978, 3160977, 3160955,
                        3160949, 3160947, 3160944, 3160942, 3160941, 3160939, 3160938,
                        3160989, 3160990, 3160967, 3161018, 3758511, 3758512, 11271876, 11271878]
    }
    
    for region, gauge_ids in regions.items():
        print(f"\n  {region}:")
        for gid in gauge_ids:
            if gid in KNOWN_GAUGE_LOCATIONS:
                lon, lat = KNOWN_GAUGE_LOCATIONS[gid]
                # Get name from first part of location description
                name = get_gauge_name_hint(gid)
                print(f"    {gid:<10} {name:<53} {lon:>8.2f} {lat:>8.2f}")
    
    print("\n" + "="*85)
    print(f"Total: {len(KNOWN_GAUGE_LOCATIONS)} gauges with known coordinates")
    print("\nUsage examples:")
    print("  python cross_validate_gauge_radar.py --gauge-id 3160950 --date 2026-01-21")
    print("  python cross_validate_gauge_radar.py --gauge-ids 3160950,9599440,3160966 --date 2026-01-21")
    print("="*85 + "\n")


def get_gauge_name_hint(gauge_id: int) -> str:
    """Get a short name hint for gauge ID."""
    GAUGE_NAMES = {
        3160950: "Swanson @ Waitakere Filter Station",
        3160946: "Te Pai Park Henderson",
        3160982: "Cutler Park New Lynn",
        3160962: "Opanuku @ Candia Road",
        3160961: "Oratia Cemetery",
        3160973: "Kumeu @ Waitakere Domain",
        3160974: "Kumeu @ Maddrens",
        3160935: "Waiatarua Rainfall",
        3160980: "Forrest Hill Road Waiatarua",
        3160979: "Harmel Road Waitakere",
        3160984: "Constable Ln Waitakere",
        9601912: "Piha Rangers Shed",
        11271877: "Piha Wetlands",
        7828282: "Nihotupu @ Arataki",
        7828283: "Waituna @ Huia Filter Station",
        3160995: "Albert Park",
        3160966: "Mt Albert Grammar",
        3160963: "Okahu Bay Bowling Club",
        3160991: "Avondale Racecourse",
        3160937: "Whau @ Mt Roskill",
        3160983: "Cox's Bay Park",
        3160971: "Longford Park",
        11315277: "Takapuna Rain @ Library",
        3160996: "Albany @ Hts Rd",
        3160953: "School @ Mairangi Bay",
        3160945: "Torbay @ Glamorgan School",
        3160986: "Birkdale (Inwards Res.)",
        3160987: "Bayswater @ Plymouth Res.",
        3160943: "Wairau at Testing Station",
        3160960: "Oteha @ Rosedale Ponds",
        9599440: "Mangere Rainfall",
        34966199: "Mangere @ Greenwood Road",
        7828285: "Manukau @ Sports Bowl",
        7828287: "Drury @ Turner Road",
        3160958: "Papakura Rain @ Kaipara Rd",
        3160985: "Clevedon rainfall @ showgrounds",
        7828286: "Clevedon Coast RAWS",
        11271879: "Puhinui @ Botanical Gardens",
        11271880: "Karaka @ Walters Road",
        11271881: "Mauku RAWS",
        33236625: "Pakuranga @ College",
        3160936: "Whenuapai @ Airbase",
    }
    return GAUGE_NAMES.get(gauge_id, f"Gauge {gauge_id}")


def main() -> int:
    """Main entry point."""
    args = parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # List mode: show available gauges and exit
    if args.list:
        list_available_gauges()
        return 0
    
    # Validate date is provided for non-list mode
    if not args.date:
        logger.error("--date is required. Use --list to see available gauges.")
        return 1
    
    # Check spatial libraries
    if not SPATIAL_AVAILABLE:
        logger.error(
            "This script requires pyproj and shapely for coordinate transformations.\n"
            "Install with: pip install pyproj shapely"
        )
        return 1
    
    # Parse gauge IDs
    gauge_ids_to_process = []
    
    if args.batch:
        gauge_ids_to_process = list(KNOWN_GAUGE_LOCATIONS.keys())
    elif args.gauge_ids:
        # Parse comma-separated list
        try:
            gauge_ids_to_process = [int(gid.strip()) for gid in args.gauge_ids.split(",")]
        except ValueError:
            logger.error("Invalid gauge ID format. Use comma-separated integers (e.g., 3160950,9599440)")
            return 1
    elif args.gauge_id:
        gauge_ids_to_process = [args.gauge_id]
    else:
        logger.error("Specify --gauge-id, --gauge-ids, --batch, or --list")
        return 1
    
    # Check if manual coordinates provided for single gauge mode
    manual_coords = None
    if args.lon is not None and args.lat is not None:
        manual_coords = (args.lon, args.lat)
        logger.info(f"Using manual coordinates: lon={args.lon}, lat={args.lat}")
    
    # Run for multiple gauges if more than one
    if len(gauge_ids_to_process) > 1:
        if manual_coords:
            logger.warning("--lat/--lon ignored for multi-gauge mode. Using KNOWN_GAUGE_LOCATIONS.")
        return run_multi_gauge_validation(args, gauge_ids_to_process)
    
    # Single gauge mode
    # Prepare gauge coordinates - manual overrides known locations
    gauge_coords = None
    gauge_id = gauge_ids_to_process[0]
    
    if manual_coords:
        gauge_coords = manual_coords
        logger.info(f"Using MANUAL coordinates for gauge {gauge_id}: {gauge_coords}")
    elif gauge_id in KNOWN_GAUGE_LOCATIONS:
        gauge_coords = KNOWN_GAUGE_LOCATIONS[gauge_id]
        logger.info(f"Using KNOWN coordinates for gauge {gauge_id}: {gauge_coords}")
    else:
        logger.error(f"Gauge {gauge_id} not in KNOWN_GAUGE_LOCATIONS.")
        logger.error(f"Please provide coordinates manually with --lat and --lon")
        logger.error(f"Example: python cross_validate_gauge_radar.py --gauge-id {gauge_id} --date {args.date} --lat -36.87 --lon 174.58")
        return 1
    
    # Prepare output paths
    output_file = Path(args.output) if args.output else None
    html_output = Path(args.html) if args.html else None
    
    # Auto-generate HTML path if not specified
    if html_output is None:
        # Create default HTML output path
        html_output = Path(f"outputs/cross_validation/gauge_{gauge_id}_{args.date}_report.html")
    
    # Run cross-validation
    try:
        result = cross_validate_gauge_radar(
            gauge_id=gauge_id,
            date_str=args.date,
            radius_meters=args.radius,
            gauge_coords=gauge_coords,
            aggregation_method=args.aggregation,
            output_file=output_file,
            html_output=html_output
        )
        
        if result is None:
            logger.error("Cross-validation failed")
            return 1
        
        # Open HTML in browser if generated
        if html_output and html_output.exists():
            import webbrowser
            webbrowser.open(str(html_output.absolute()))
            logger.info(f"Opened dashboard in browser: {html_output}")
        
        logger.info("Cross-validation completed successfully")
        return 0
        
    except Exception as e:
        logger.exception(f"Cross-validation failed with error: {e}")
        return 1


def run_multi_gauge_validation(args, gauge_ids: List[int]) -> int:
    """
    Run cross-validation for multiple selected gauges.
    
    Args:
        args: Parsed command line arguments
        gauge_ids: List of gauge IDs to process
        
    Returns:
        Exit code (0 for success)
    """
    logger = logging.getLogger(__name__)
    
    # Results storage
    all_results = []
    successful = 0
    failed = 0
    skipped = 0
    
    total = len(gauge_ids)
    logger.info(f"\n{'='*70}")
    logger.info(f"CROSS-VALIDATION: {total} gauges selected")
    logger.info(f"{'='*70}\n")
    
    # Create output directory for individual reports
    output_dir = Path("outputs/cross_validation")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for idx, gauge_id in enumerate(gauge_ids, 1):
        # Check if we have coordinates
        if gauge_id not in KNOWN_GAUGE_LOCATIONS:
            logger.warning(f"[{idx}/{total}] Gauge {gauge_id}: Unknown coordinates, skipping")
            skipped += 1
            continue
        
        lon, lat = KNOWN_GAUGE_LOCATIONS[gauge_id]
        name = get_gauge_name_hint(gauge_id)
        logger.info(f"\n[{idx}/{total}] {name} ({gauge_id})...")
        
        # Generate individual HTML report for each gauge
        individual_html = output_dir / f"gauge_{gauge_id}_{args.date}_report.html"
        
        try:
            result = cross_validate_gauge_radar(
                gauge_id=gauge_id,
                date_str=args.date,
                radius_meters=args.radius,
                gauge_coords=(lon, lat),
                aggregation_method=args.aggregation,
                output_file=None,
                html_output=individual_html  # Generate individual HTML
            )
            
            if result is None:
                logger.warning(f"  No data or validation failed")
                skipped += 1
                continue
            
            # Extract metrics
            metrics = result.get("metrics", {})
            corr = metrics.get("correlation", np.nan)
            bias = metrics.get("percent_bias", np.nan)
            nse = metrics.get("nse", np.nan)
            
            all_results.append({
                "gauge_id": gauge_id,
                "name": name,
                "lon": lon,
                "lat": lat,
                "samples": result.get("n_matched_samples", 0),
                "r": corr,
                "r2": metrics.get("r_squared", np.nan),
                "nse": nse,
                "bias_pct": bias,
                "rmse": metrics.get("rmse", np.nan),
                "html_file": individual_html.name,  # Store filename for linking
            })
            
            r_str = f"{corr:.2f}" if not np.isnan(corr) else "N/A"
            bias_str = f"{bias:+.1f}%" if not np.isnan(bias) else "N/A"
            nse_str = f"{nse:.2f}" if not np.isnan(nse) else "N/A"
            rating = "Good" if corr >= 0.7 else "Moderate" if corr >= 0.5 else "Poor" if not np.isnan(corr) else "N/A"
            
            logger.info(f"  r={r_str} ({rating}), Bias={bias_str}, NSE={nse_str}")
            successful += 1
            
        except Exception as e:
            logger.warning(f"  Error: {e}")
            failed += 1
    
    # Print summary table
    print(f"\n{'='*90}")
    print("CROSS-VALIDATION SUMMARY")
    print(f"{'='*90}")
    print(f"{'ID':<10} {'Name':<35} {'r':>6} {'R²':>6} {'NSE':>7} {'Bias%':>8} {'Rating':>10}")
    print("-"*90)
    
    for res in sorted(all_results, key=lambda x: x.get("r", -999) if not np.isnan(x.get("r", np.nan)) else -999, reverse=True):
        r = res['r']
        rating = "Good" if r >= 0.7 else "Moderate" if r >= 0.5 else "Poor" if not np.isnan(r) else "N/A"
        print(f"{res['gauge_id']:<10} {res['name'][:34]:<35} {r:>6.2f} {res['r2']:>6.2f} {res['nse']:>7.2f} {res['bias_pct']:>+7.1f}% {rating:>10}")
    
    print("-"*90)
    print(f"Total: {successful} successful, {skipped} skipped, {failed} failed")
    print(f"{'='*90}\n")
    
    # Save to CSV and HTML
    if all_results:
        output_dir = Path("outputs/cross_validation")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save CSV
        csv_path = output_dir / f"batch_validation_{args.date}.csv"
        pd.DataFrame(all_results).to_csv(csv_path, index=False)
        logger.info(f"Saved CSV results to: {csv_path}")
        
        # Generate HTML dashboard
        html_path = output_dir / f"batch_validation_{args.date}_report.html"
        generate_batch_html_dashboard(all_results, args.date, html_path)
        logger.info(f"Generated HTML dashboard: {html_path}")
        
        # Open in browser
        import webbrowser
        webbrowser.open(str(html_path.absolute()))
    
    return 0


def generate_batch_html_dashboard(results: List[Dict], date_str: str, output_path: Path):
    """Generate HTML dashboard for batch validation results."""
    
    # Sort by correlation (best first)
    sorted_results = sorted(results, key=lambda x: x.get("r", -999) if not np.isnan(x.get("r", np.nan)) else -999, reverse=True)
    
    # Calculate summary stats
    r_values = [r['r'] for r in results if not np.isnan(r.get('r', np.nan))]
    bias_values = [r['bias_pct'] for r in results if not np.isnan(r.get('bias_pct', np.nan))]
    nse_values = [r['nse'] for r in results if not np.isnan(r.get('nse', np.nan))]
    
    avg_r = np.mean(r_values) if r_values else 0
    avg_bias = np.mean(bias_values) if bias_values else 0
    avg_nse = np.mean(nse_values) if nse_values else 0
    
    good_count = len([r for r in r_values if r >= 0.7])
    moderate_count = len([r for r in r_values if 0.5 <= r < 0.7])
    poor_count = len([r for r in r_values if r < 0.5])
    
    # Build table rows
    table_rows = ""
    for res in sorted_results:
        r = res.get('r', np.nan)
        r2 = res.get('r2', np.nan)
        nse = res.get('nse', np.nan)
        bias = res.get('bias_pct', np.nan)
        rmse = res.get('rmse', np.nan)
        
        if np.isnan(r):
            rating = "N/A"
            rating_class = "poor"
        elif r >= 0.7:
            rating = "Good"
            rating_class = "good"
        elif r >= 0.5:
            rating = "Moderate"
            rating_class = "moderate"
        else:
            rating = "Poor"
            rating_class = "poor"
        
        r_str = f"{r:.3f}" if not np.isnan(r) else "N/A"
        r2_str = f"{r2:.3f}" if not np.isnan(r2) else "N/A"
        nse_str = f"{nse:.3f}" if not np.isnan(nse) else "N/A"
        bias_str = f"{bias:+.1f}%" if not np.isnan(bias) else "N/A"
        rmse_str = f"{rmse:.3f}" if not np.isnan(rmse) else "N/A"
        
        # Get individual report link
        html_file = res.get('html_file', '')
        detail_link = f'<a href="{html_file}" class="detail-link" title="View detailed report">📊 Details</a>' if html_file else ''
        
        table_rows += f"""
        <tr class="{rating_class}" onclick="window.location='{html_file}'" style="cursor: pointer;">
            <td><a href="{html_file}" class="gauge-link">{res['gauge_id']}</a></td>
            <td><a href="{html_file}" class="gauge-link">{res['name']}</a></td>
            <td>{res.get('lon', 'N/A'):.4f}</td>
            <td>{res.get('lat', 'N/A'):.4f}</td>
            <td>{res.get('samples', 0)}</td>
            <td><strong>{r_str}</strong></td>
            <td>{r2_str}</td>
            <td>{nse_str}</td>
            <td>{bias_str}</td>
            <td>{rmse_str}</td>
            <td><span class="rating {rating_class}">{rating}</span></td>
            <td>{detail_link}</td>
        </tr>"""
    
    # Prepare chart data
    gauge_labels = [f"{r['gauge_id']}" for r in sorted_results]
    r_data = [round(r['r'], 3) if not np.isnan(r.get('r', np.nan)) else 0 for r in sorted_results]
    bias_data = [round(r['bias_pct'], 1) if not np.isnan(r.get('bias_pct', np.nan)) else 0 for r in sorted_results]
    nse_data = [round(r['nse'], 3) if not np.isnan(r.get('nse', np.nan)) else 0 for r in sorted_results]
    
    # Convert to JSON strings for JavaScript
    gauge_labels_json = json.dumps(gauge_labels)
    r_data_json = json.dumps(r_data)
    bias_data_json = json.dumps(bias_data)
    nse_data_json = json.dumps(nse_data)
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Batch Cross-Validation Report - {date_str}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f7fa; color: #333; line-height: 1.6; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        h1 {{ text-align: center; color: #2c3e50; margin-bottom: 10px; }}
        .subtitle {{ text-align: center; color: #7f8c8d; margin-bottom: 30px; }}
        
        .summary-cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }}
        .card h3 {{ color: #7f8c8d; font-size: 0.9em; margin-bottom: 10px; }}
        .card .value {{ font-size: 2em; font-weight: bold; }}
        .card .value.good {{ color: #27ae60; }}
        .card .value.moderate {{ color: #f39c12; }}
        .card .value.poor {{ color: #e74c3c; }}
        
        .charts-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .chart-container h3 {{ margin-bottom: 15px; color: #2c3e50; }}
        
        table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        th {{ background: #2c3e50; color: white; padding: 12px 8px; text-align: left; font-size: 0.85em; }}
        td {{ padding: 10px 8px; border-bottom: 1px solid #ecf0f1; font-size: 0.85em; }}
        tr:hover {{ background: #f8f9fa; }}
        tr.good {{ background: rgba(39, 174, 96, 0.1); }}
        tr.moderate {{ background: rgba(243, 156, 18, 0.1); }}
        tr.poor {{ background: rgba(231, 76, 60, 0.05); }}
        
        .rating {{ padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 0.8em; }}
        .rating.good {{ background: #27ae60; color: white; }}
        .rating.moderate {{ background: #f39c12; color: white; }}
        .rating.poor {{ background: #e74c3c; color: white; }}
        
        .legend {{ display: flex; justify-content: center; gap: 30px; margin: 20px 0; flex-wrap: wrap; }}
        .legend-item {{ display: flex; align-items: center; gap: 8px; }}
        .legend-color {{ width: 20px; height: 20px; border-radius: 4px; }}
        
        .gauge-link {{ color: #3498db; text-decoration: none; font-weight: 500; }}
        .gauge-link:hover {{ text-decoration: underline; color: #2980b9; }}
        .detail-link {{ display: inline-block; padding: 4px 10px; background: #3498db; color: white; border-radius: 4px; text-decoration: none; font-size: 0.8em; }}
        .detail-link:hover {{ background: #2980b9; }}
        tr:hover {{ background: #e8f4fc !important; }}
        
        footer {{ text-align: center; color: #7f8c8d; margin-top: 30px; padding: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🌧️ Batch Cross-Validation Report</h1>
        <p class="subtitle">Rain Gauge vs Radar Comparison | Date: {date_str} | {len(results)} Gauges Analyzed</p>
        <p class="subtitle" style="font-size: 0.9em; margin-top: -20px;">💡 Click on any row to view detailed timeseries report</p>
        
        <div class="summary-cards">
            <div class="card">
                <h3>Total Gauges</h3>
                <div class="value">{len(results)}</div>
            </div>
            <div class="card">
                <h3>Good (r ≥ 0.7)</h3>
                <div class="value good">{good_count}</div>
            </div>
            <div class="card">
                <h3>Moderate (r 0.5-0.7)</h3>
                <div class="value moderate">{moderate_count}</div>
            </div>
            <div class="card">
                <h3>Poor (r &lt; 0.5)</h3>
                <div class="value poor">{poor_count}</div>
            </div>
            <div class="card">
                <h3>Average Correlation</h3>
                <div class="value">{avg_r:.2f}</div>
            </div>
            <div class="card">
                <h3>Average Bias</h3>
                <div class="value">{avg_bias:+.1f}%</div>
            </div>
        </div>
        
        <div class="charts-row">
            <div class="chart-container">
                <h3>📊 Correlation by Gauge</h3>
                <canvas id="correlationChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>📉 Bias by Gauge (%)</h3>
                <canvas id="biasChart"></canvas>
            </div>
        </div>
        
        <div class="chart-container" style="margin-bottom: 30px;">
            <h3>📈 Nash-Sutcliffe Efficiency (NSE) by Gauge</h3>
            <canvas id="nseChart"></canvas>
        </div>
        
        <div class="legend">
            <div class="legend-item"><div class="legend-color" style="background: #27ae60;"></div><span>Good (r ≥ 0.7)</span></div>
            <div class="legend-item"><div class="legend-color" style="background: #f39c12;"></div><span>Moderate (r 0.5-0.7)</span></div>
            <div class="legend-item"><div class="legend-color" style="background: #e74c3c;"></div><span>Poor (r &lt; 0.5)</span></div>
        </div>
        
        <h2 style="margin-bottom: 15px;">📋 Detailed Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Gauge ID</th>
                    <th>Name</th>
                    <th>Lon</th>
                    <th>Lat</th>
                    <th>Samples</th>
                    <th>Corr (r)</th>
                    <th>R²</th>
                    <th>NSE</th>
                    <th>Bias</th>
                    <th>RMSE</th>
                    <th>Rating</th>
                    <th>Details</th>
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>
        
        <footer>
            <p>Generated by Auckland Council Rain Monitoring System</p>
            <p>Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </footer>
    </div>
    
    <script>
        // Correlation Chart
        const gaugeLabels = {gauge_labels_json};
        const rData = {r_data_json};
        const biasData = {bias_data_json};
        const nseData = {nse_data_json};
        
        new Chart(document.getElementById('correlationChart'), {{
            type: 'bar',
            data: {{
                labels: gaugeLabels,
                datasets: [{{
                    label: 'Pearson Correlation (r)',
                    data: rData,
                    backgroundColor: rData.map(v => v >= 0.7 ? '#27ae60' : v >= 0.5 ? '#f39c12' : '#e74c3c'),
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{ min: 0, max: 1, title: {{ display: true, text: 'Correlation (r)' }} }},
                    x: {{ title: {{ display: true, text: 'Gauge ID' }} }}
                }},
                plugins: {{
                    legend: {{ display: false }}
                }}
            }}
        }});
        
        // Bias Chart
        new Chart(document.getElementById('biasChart'), {{
            type: 'bar',
            data: {{
                labels: gaugeLabels,
                datasets: [{{
                    label: 'Percent Bias (%)',
                    data: biasData,
                    backgroundColor: biasData.map(v => Math.abs(v) <= 10 ? '#27ae60' : Math.abs(v) <= 25 ? '#f39c12' : '#e74c3c'),
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{ title: {{ display: true, text: 'Bias (%)' }} }},
                    x: {{ title: {{ display: true, text: 'Gauge ID' }} }}
                }},
                plugins: {{
                    legend: {{ display: false }}
                }}
            }}
        }});
        
        // NSE Chart
        new Chart(document.getElementById('nseChart'), {{
            type: 'bar',
            data: {{
                labels: gaugeLabels,
                datasets: [{{
                    label: 'Nash-Sutcliffe Efficiency',
                    data: nseData,
                    backgroundColor: nseData.map(v => v >= 0.5 ? '#27ae60' : v >= 0 ? '#f39c12' : '#e74c3c'),
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{ title: {{ display: true, text: 'NSE' }} }},
                    x: {{ title: {{ display: true, text: 'Gauge ID' }} }}
                }},
                plugins: {{
                    legend: {{ display: false }}
                }}
            }}
        }});
    </script>
</body>
</html>"""
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)


def run_batch_validation(args) -> int:
    """
    Run cross-validation for all gauges with known coordinates.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success)
    """
    logger = logging.getLogger(__name__)
    
    # Results storage
    all_results = []
    successful = 0
    failed = 0
    skipped = 0
    
    total_gauges = len(KNOWN_GAUGE_LOCATIONS)
    logger.info(f"\n{'='*70}")
    logger.info(f"BATCH CROSS-VALIDATION: {total_gauges} gauges")
    logger.info(f"{'='*70}\n")
    
    for idx, (gauge_id, (lon, lat)) in enumerate(KNOWN_GAUGE_LOCATIONS.items(), 1):
        logger.info(f"\n[{idx}/{total_gauges}] Processing gauge {gauge_id}...")
        
        try:
            # Run cross-validation (without HTML to speed up)
            result = cross_validate_gauge_radar(
                gauge_id=gauge_id,
                date_str=args.date,
                radius_meters=args.radius,
                gauge_coords=(lon, lat),
                aggregation_method=args.aggregation,
                output_file=None,
                html_output=None  # Skip individual HTML generation
            )
            
            if result is None:
                logger.warning(f"  Gauge {gauge_id}: No data or validation failed")
                skipped += 1
                continue
            
            # Extract metrics
            metrics = result.get("metrics", {})
            gauge_info = result
            
            # Get correlation value
            corr_value = metrics.get("correlation", np.nan)
            
            all_results.append({
                "gauge_id": gauge_id,
                "gauge_name": result.get("gauge_name", f"Gauge {gauge_id}"),
                "longitude": lon,
                "latitude": lat,
                "matched_samples": result.get("n_matched_samples", metrics.get("n_samples", 0)),
                "nearby_pixels": result.get("n_nearby_pixels", 0),
                "correlation_r": corr_value,
                "r_squared": metrics.get("r_squared", np.nan),
                "nse": metrics.get("nse", np.nan),
                "rmse_mm": metrics.get("rmse", np.nan),
                "mae_mm": metrics.get("mae", np.nan),
                "bias_mm": metrics.get("bias", np.nan),
                "percent_bias": metrics.get("percent_bias", np.nan),
                "gauge_mean_mm": metrics.get("gauge_mean", np.nan),
                "radar_mean_mm": metrics.get("radar_mean", np.nan),
            })
            
            r = corr_value if not np.isnan(corr_value) else 0
            rating = "Good" if r >= 0.7 else "Moderate" if r >= 0.5 else "Poor"
            logger.info(f"  Gauge {gauge_id}: r={r:.3f} ({rating}), Bias={metrics.get('percent_bias', 0):.1f}%")
            successful += 1
            
        except Exception as e:
            logger.warning(f"  Gauge {gauge_id}: Error - {e}")
            failed += 1
            continue
    
    # Create summary DataFrame
    if all_results:
        df_results = pd.DataFrame(all_results)
        
        # Sort by correlation (best first)
        df_results = df_results.sort_values("correlation_r", ascending=False)
        
        # Save to CSV
        output_dir = Path("outputs/cross_validation")
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"batch_validation_{args.date}.csv"
        df_results.to_csv(csv_path, index=False)
        logger.info(f"\nResults saved to: {csv_path}")
        
        # Generate summary HTML report
        html_path = output_dir / f"batch_validation_{args.date}_report.html"
        generate_batch_html_report(df_results, args.date, html_path)
        logger.info(f"HTML report saved to: {html_path}")
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"BATCH CROSS-VALIDATION SUMMARY: {args.date}")
        print(f"{'='*80}")
        print(f"Total gauges: {total_gauges}")
        print(f"Successful: {successful}")
        print(f"Skipped (no data): {skipped}")
        print(f"Failed (errors): {failed}")
        print()
        
        # Statistics summary
        print(f"{'='*80}")
        print("PERFORMANCE STATISTICS")
        print(f"{'='*80}")
        print(f"Mean Correlation (r):     {df_results['correlation_r'].mean():.4f}")
        print(f"Median Correlation (r):   {df_results['correlation_r'].median():.4f}")
        print(f"Best Correlation:         {df_results['correlation_r'].max():.4f}")
        print(f"Worst Correlation:        {df_results['correlation_r'].min():.4f}")
        print()
        print(f"Mean Bias (%):            {df_results['percent_bias'].mean():.2f}")
        print(f"Mean RMSE (mm):           {df_results['rmse_mm'].mean():.4f}")
        print(f"Mean MAE (mm):            {df_results['mae_mm'].mean():.4f}")
        print()
        
        # Rating distribution
        good = (df_results['correlation_r'] >= 0.7).sum()
        moderate = ((df_results['correlation_r'] >= 0.5) & (df_results['correlation_r'] < 0.7)).sum()
        poor = (df_results['correlation_r'] < 0.5).sum()
        
        print(f"{'='*80}")
        print("RATING DISTRIBUTION")
        print(f"{'='*80}")
        print(f"Good (r >= 0.7):          {good} ({100*good/len(df_results):.1f}%)")
        print(f"Moderate (0.5 <= r < 0.7): {moderate} ({100*moderate/len(df_results):.1f}%)")
        print(f"Poor (r < 0.5):           {poor} ({100*poor/len(df_results):.1f}%)")
        print()
        
        # Top 10 best gauges
        print(f"{'='*80}")
        print("TOP 10 BEST PERFORMING GAUGES")
        print(f"{'='*80}")
        print(f"{'Rank':<5} {'ID':<10} {'Name':<40} {'r':>8} {'Bias%':>8}")
        print("-" * 75)
        for i, row in df_results.head(10).iterrows():
            name = row['gauge_name'][:39] if len(row['gauge_name']) > 39 else row['gauge_name']
            print(f"{df_results.index.get_loc(i)+1:<5} {row['gauge_id']:<10} {name:<40} {row['correlation_r']:>8.3f} {row['percent_bias']:>7.1f}%")
        print()
        
        # Open HTML in browser
        import webbrowser
        webbrowser.open(str(html_path.absolute()))
        logger.info(f"Opened batch report in browser")
    
    return 0


def generate_batch_html_report(df: pd.DataFrame, date_str: str, output_path: Path) -> None:
    """
    Generate an HTML report for batch validation results.
    
    Args:
        df: DataFrame with validation results
        date_str: Date string
        output_path: Output HTML file path
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Calculate statistics
    mean_r = df['correlation_r'].mean()
    median_r = df['correlation_r'].median()
    max_r = df['correlation_r'].max()
    min_r = df['correlation_r'].min()
    mean_bias = df['percent_bias'].mean()
    mean_rmse = df['rmse_mm'].mean()
    
    good = (df['correlation_r'] >= 0.7).sum()
    moderate = ((df['correlation_r'] >= 0.5) & (df['correlation_r'] < 0.7)).sum()
    poor = (df['correlation_r'] < 0.5).sum()
    total = len(df)
    
    # Generate table rows
    table_rows = []
    for idx, row in df.iterrows():
        r = row['correlation_r']
        rating = "Good" if r >= 0.7 else "Moderate" if r >= 0.5 else "Poor"
        color = "#28a745" if r >= 0.7 else "#ffc107" if r >= 0.5 else "#dc3545"
        
        table_rows.append(f"""
            <tr>
                <td>{row['gauge_id']}</td>
                <td>{row['gauge_name']}</td>
                <td style="color: {color}; font-weight: bold;">{r:.3f}</td>
                <td>{row['r_squared']:.3f}</td>
                <td>{row['nse']:.3f}</td>
                <td>{row['rmse_mm']:.3f}</td>
                <td>{row['bias_mm']:.3f}</td>
                <td>{row['percent_bias']:.1f}%</td>
                <td>{row['matched_samples']}</td>
                <td style="color: {color};">{rating}</td>
            </tr>
        """)
    
    table_html = "\n".join(table_rows)
    
    # Prepare data for charts
    gauge_names = df['gauge_name'].str[:20].tolist()
    correlations = df['correlation_r'].tolist()
    biases = df['percent_bias'].tolist()
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Batch Cross-Validation Report - {date_str}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            text-align: center;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            color: #1e3c72;
        }}
        .stat-label {{
            color: #666;
            margin-top: 5px;
        }}
        .chart-container {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .charts-row {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background: #1e3c72;
            color: white;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .rating-chart {{
            max-width: 300px;
            margin: 0 auto;
        }}
        @media (max-width: 768px) {{
            .charts-row {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🌧️ Batch Cross-Validation Report</h1>
        <h2>Rain Gauge vs Radar Data - {date_str}</h2>
        <p>Auckland Council Rain Monitoring System</p>
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-value">{total}</div>
            <div class="stat-label">Total Gauges</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" style="color: #28a745;">{good}</div>
            <div class="stat-label">Good (r ≥ 0.7)</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" style="color: #ffc107;">{moderate}</div>
            <div class="stat-label">Moderate (r 0.5-0.7)</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" style="color: #dc3545;">{poor}</div>
            <div class="stat-label">Poor (r < 0.5)</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{mean_r:.3f}</div>
            <div class="stat-label">Mean Correlation</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{mean_bias:.1f}%</div>
            <div class="stat-label">Mean Bias</div>
        </div>
    </div>
    
    <div class="charts-row">
        <div class="chart-container">
            <h3>Correlation by Gauge</h3>
            <canvas id="correlationChart"></canvas>
        </div>
        <div class="chart-container">
            <h3>Bias (%) by Gauge</h3>
            <canvas id="biasChart"></canvas>
        </div>
    </div>
    
    <div class="charts-row">
        <div class="chart-container">
            <h3>Rating Distribution</h3>
            <div class="rating-chart">
                <canvas id="ratingChart"></canvas>
            </div>
        </div>
        <div class="chart-container">
            <h3>Correlation Distribution</h3>
            <canvas id="histogramChart"></canvas>
        </div>
    </div>
    
    <div class="chart-container">
        <h3>All Gauges - Detailed Results</h3>
        <table>
            <thead>
                <tr>
                    <th>Gauge ID</th>
                    <th>Name</th>
                    <th>Corr (r)</th>
                    <th>R²</th>
                    <th>NSE</th>
                    <th>RMSE</th>
                    <th>Bias</th>
                    <th>Bias %</th>
                    <th>Samples</th>
                    <th>Rating</th>
                </tr>
            </thead>
            <tbody>
                {table_html}
            </tbody>
        </table>
    </div>
    
    <script>
        // Correlation Chart
        new Chart(document.getElementById('correlationChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(gauge_names)},
                datasets: [{{
                    label: 'Correlation (r)',
                    data: {json.dumps(correlations)},
                    backgroundColor: {json.dumps(correlations)}.map(r => 
                        r >= 0.7 ? '#28a745' : r >= 0.5 ? '#ffc107' : '#dc3545'
                    ),
                }}]
            }},
            options: {{
                indexAxis: 'y',
                responsive: true,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    x: {{ min: 0, max: 1 }}
                }}
            }}
        }});
        
        // Bias Chart
        new Chart(document.getElementById('biasChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(gauge_names)},
                datasets: [{{
                    label: 'Bias (%)',
                    data: {json.dumps(biases)},
                    backgroundColor: {json.dumps(biases)}.map(b => 
                        Math.abs(b) <= 10 ? '#28a745' : Math.abs(b) <= 25 ? '#ffc107' : '#dc3545'
                    ),
                }}]
            }},
            options: {{
                indexAxis: 'y',
                responsive: true,
                plugins: {{
                    legend: {{ display: false }}
                }}
            }}
        }});
        
        // Rating Pie Chart
        new Chart(document.getElementById('ratingChart'), {{
            type: 'doughnut',
            data: {{
                labels: ['Good (r ≥ 0.7)', 'Moderate (0.5-0.7)', 'Poor (r < 0.5)'],
                datasets: [{{
                    data: [{good}, {moderate}, {poor}],
                    backgroundColor: ['#28a745', '#ffc107', '#dc3545'],
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'bottom' }}
                }}
            }}
        }});
        
        // Histogram
        const correlations = {json.dumps(correlations)};
        const bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
        const histogram = new Array(bins.length - 1).fill(0);
        correlations.forEach(r => {{
            for (let i = 0; i < bins.length - 1; i++) {{
                if (r >= bins[i] && r < bins[i+1]) {{
                    histogram[i]++;
                    break;
                }}
            }}
        }});
        
        new Chart(document.getElementById('histogramChart'), {{
            type: 'bar',
            data: {{
                labels: bins.slice(0, -1).map((b, i) => `${{b.toFixed(1)}}-${{bins[i+1].toFixed(1)}}`),
                datasets: [{{
                    label: 'Number of Gauges',
                    data: histogram,
                    backgroundColor: bins.slice(0, -1).map(b => 
                        b >= 0.7 ? '#28a745' : b >= 0.5 ? '#ffc107' : '#dc3545'
                    ),
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)


if __name__ == "__main__":
    sys.exit(main())
