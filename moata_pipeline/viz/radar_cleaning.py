"""
Data loading and cleaning for radar dashboard.
"""
from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


def load_catchments(catchments_dir: Path) -> pd.DataFrame:
    """Load catchments data."""
    file_path = catchments_dir / "stormwater_catchments.csv"
    if not file_path.exists():
        logger.warning("Catchments file not found: %s", file_path)
        return pd.DataFrame()
    return pd.read_csv(file_path)


def load_pixel_mappings(mappings_dir: Path) -> Dict[int, List[int]]:
    """Load pixel mappings from pickle or JSON."""
    pkl_path = mappings_dir / "catchment_pixel_mapping.pkl"
    json_path = mappings_dir / "catchment_pixel_mapping.json"
    
    if pkl_path.exists():
        with open(pkl_path, "rb") as f:
            return pickle.load(f)
    elif json_path.exists():
        with open(json_path, "r") as f:
            data = json.load(f)
        return {int(k): v for k, v in data.items()}
    else:
        logger.warning("Pixel mappings not found")
        return {}


def analyze_catchment(
    radar_dir: Path,
    catchment_id: int,
    catchment_name: str,
    pixel_count: int,
) -> Dict:
    """Analyze radar data for one catchment."""
    radar_files = list(radar_dir.glob(f"{catchment_id}_*.csv"))
    
    if not radar_files:
        return {
            "catchment_id": catchment_id,
            "catchment_name": catchment_name,
            "pixel_count": pixel_count,
            "has_data": False,
            "total_rainfall": 0,
            "avg_rainfall_per_pixel": 0,
            "max_intensity": 0,
            "pixels_with_rain": 0,
            "rain_coverage_pct": 0,
        }
    
    df = pd.read_csv(radar_files[0])
    
    pixel_stats = df.groupby("pixel_index").agg({
        "value": ["sum", "max", "count"]
    }).reset_index()
    pixel_stats.columns = ["pixel_index", "total", "max", "count"]
    
    total_rainfall = pixel_stats["total"].sum()
    pixels_with_rain = (pixel_stats["total"] > 0).sum()
    
    return {
        "catchment_id": catchment_id,
        "catchment_name": catchment_name,
        "pixel_count": pixel_count,
        "has_data": True,
        "total_rainfall": round(total_rainfall, 2),
        "avg_rainfall_per_pixel": round(total_rainfall / len(pixel_stats), 3) if len(pixel_stats) > 0 else 0,
        "max_intensity": round(pixel_stats["max"].max(), 3),
        "pixels_with_rain": int(pixels_with_rain),
        "rain_coverage_pct": round(100 * pixels_with_rain / len(pixel_stats), 1) if len(pixel_stats) > 0 else 0,
    }


def load_and_analyze(data_dir: Path) -> pd.DataFrame:
    """Load radar data and analyze all catchments."""
    catchments_dir = data_dir / "catchments"
    mappings_dir = data_dir / "pixel_mappings"
    radar_dir = data_dir / "radar_data"
    
    catchments = load_catchments(catchments_dir)
    pixel_mappings = load_pixel_mappings(mappings_dir)
    
    if catchments.empty or not pixel_mappings:
        logger.error("No data to analyze")
        return pd.DataFrame()
    
    logger.info("Analyzing %d catchments...", len(pixel_mappings))
    
    stats = []
    total = len(pixel_mappings)
    
    for idx, (catchment_id, pixels) in enumerate(pixel_mappings.items(), 1):
        catchment_row = catchments[catchments["id"] == catchment_id]
        catchment_name = catchment_row["name"].values[0] if len(catchment_row) > 0 else f"ID_{catchment_id}"
        
        stat = analyze_catchment(radar_dir, catchment_id, catchment_name, len(pixels))
        stats.append(stat)
        
        if idx % 50 == 0:
            logger.info("  Progress: %d/%d", idx, total)
    
    logger.info("✓ Analyzed %d catchments", len(stats))
    return pd.DataFrame(stats)
