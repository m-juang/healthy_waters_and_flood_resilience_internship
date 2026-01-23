"""
Radar Data Cleaning Module

Data loading and analysis for radar visualization.

UPDATED: Compatible with RadarCollector output structure:
- catchments/{id}_{name}.json (per-catchment metadata)
- radar_data/{id}_{name}.csv (per-catchment radar data)
- collection_summary.json (collection metadata)
- pixel_weights.json (optional pixel weights)

Functions:
    load_catchments: Load catchments from JSON files or collection_summary.json
    load_pixel_mappings: Load pixel mappings from catchment metadata
    analyze_catchment: Analyze single catchment
    load_and_analyze: Main analysis pipeline

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-22
Version: 2.0.0 - Compatible with RadarCollector output
"""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd


__version__ = "2.0.0"


def load_catchments_from_json_files(catchments_dir: Path) -> pd.DataFrame:
    """
    Load catchments data from individual JSON files (new format).
    
    Args:
        catchments_dir: Directory containing {id}_{name}.json files
        
    Returns:
        DataFrame with catchment data (columns: id, name, geometry, pixel_indices)
    """
    logger = logging.getLogger(__name__)
    
    if not catchments_dir.exists():
        logger.warning(f"Catchments directory not found: {catchments_dir}")
        return pd.DataFrame()
    
    json_files = list(catchments_dir.glob("*.json"))
    
    if not json_files:
        logger.warning(f"No JSON files found in: {catchments_dir}")
        return pd.DataFrame()
    
    catchments = []
    for json_file in json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            catchments.append({
                "id": data.get("id"),
                "name": data.get("name"),
                "geometry": data.get("geometry"),
                "pixel_indices": data.get("pixel_indices", []),
            })
        except Exception as e:
            logger.warning(f"Failed to load {json_file.name}: {e}")
            continue
    
    logger.info(f"Loaded {len(catchments)} catchments from JSON files")
    return pd.DataFrame(catchments)


def load_catchments_from_summary(data_dir: Path) -> pd.DataFrame:
    """
    Load catchments data from collection_summary.json (fallback method).
    
    Args:
        data_dir: Directory containing collection_summary.json
        
    Returns:
        DataFrame with catchment data
    """
    logger = logging.getLogger(__name__)
    
    summary_path = data_dir / "collection_summary.json"
    
    if not summary_path.exists():
        logger.warning(f"Collection summary not found: {summary_path}")
        return pd.DataFrame()
    
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
        
        catchments_data = summary.get("catchments", [])
        
        catchments = []
        for c in catchments_data:
            catchments.append({
                "id": c.get("catchment_id"),
                "name": c.get("catchment_name"),
                "pixel_indices": c.get("pixel_indices", []),
            })
        
        logger.info(f"Loaded {len(catchments)} catchments from collection_summary.json")
        return pd.DataFrame(catchments)
        
    except Exception as e:
        logger.warning(f"Failed to load collection summary: {e}")
        return pd.DataFrame()


def load_catchments_from_csv(catchments_dir: Path) -> pd.DataFrame:
    """
    Load catchments data from CSV (legacy format compatibility).
    
    Supports multiple filename patterns:
    - stormwater_catchments.csv (old format)
    - catchments.csv (alternate format)
    
    Args:
        catchments_dir: Directory containing catchments CSV
        
    Returns:
        DataFrame with catchment data
    """
    logger = logging.getLogger(__name__)
    
    # Try different possible filenames
    possible_files = [
        catchments_dir / "stormwater_catchments.csv",
        catchments_dir / "catchments.csv",
        catchments_dir.parent / "catchments.csv",  # Check parent directory
    ]
    
    for file_path in possible_files:
        if file_path.exists():
            logger.info(f"Loading catchments from: {file_path}")
            return pd.read_csv(file_path)
    
    logger.warning(f"No catchments CSV found in: {catchments_dir}")
    return pd.DataFrame()


def load_catchments(data_dir: Path) -> pd.DataFrame:
    """
    Load catchments data using best available method.
    
    Tries in order:
    1. Individual JSON files in catchments/ directory (new format)
    2. collection_summary.json (new format fallback)
    3. CSV file (legacy format)
    
    Args:
        data_dir: Root data directory (e.g., outputs/rain_radar/YYYYMMDD-YYYYMMDD/raw)
        
    Returns:
        DataFrame with catchment data
    """
    logger = logging.getLogger(__name__)
    
    # Method 1: Try individual JSON files (new RadarCollector format)
    catchments_dir = data_dir / "catchments"
    df = load_catchments_from_json_files(catchments_dir)
    if not df.empty:
        return df
    
    # Method 2: Try collection_summary.json
    df = load_catchments_from_summary(data_dir)
    if not df.empty:
        return df
    
    # Method 3: Try CSV (legacy format)
    df = load_catchments_from_csv(catchments_dir)
    if not df.empty:
        return df
    
    logger.error("Could not load catchments from any source")
    return pd.DataFrame()


def load_pixel_mappings_from_catchments(catchments_df: pd.DataFrame) -> Dict[int, List[int]]:
    """
    Extract pixel mappings from catchments DataFrame.
    
    Args:
        catchments_df: DataFrame with 'id' and 'pixel_indices' columns
        
    Returns:
        Dictionary mapping catchment_id to list of pixel indices
    """
    logger = logging.getLogger(__name__)
    
    if catchments_df.empty:
        return {}
    
    if "pixel_indices" not in catchments_df.columns:
        logger.warning("No pixel_indices column in catchments data")
        return {}
    
    mappings = {}
    for _, row in catchments_df.iterrows():
        catchment_id = row.get("id")
        pixel_indices = row.get("pixel_indices", [])
        
        if catchment_id is not None and pixel_indices:
            mappings[int(catchment_id)] = pixel_indices
    
    logger.info(f"Extracted pixel mappings for {len(mappings)} catchments")
    return mappings


def load_pixel_mappings_from_file(data_dir: Path) -> Dict[int, List[int]]:
    """
    Load pixel mappings from pickle or JSON file (legacy format).
    
    Supports multiple filename patterns and locations.
    
    Args:
        data_dir: Root data directory
        
    Returns:
        Dictionary mapping catchment_id to list of pixel indices
    """
    logger = logging.getLogger(__name__)
    
    # Try different possible paths
    possible_paths = [
        # New format paths
        data_dir / "pixels.pkl",
        data_dir / "pixels.json",
        # Legacy format paths
        data_dir / "pixel_mappings" / "catchment_pixel_mapping.pkl",
        data_dir / "pixel_mappings" / "catchment_pixel_mapping.json",
    ]
    
    for path in possible_paths:
        if not path.exists():
            continue
        
        try:
            if path.suffix == ".pkl":
                with open(path, "rb") as f:
                    data = pickle.load(f)
                logger.info(f"Loaded pixel mappings from: {path}")
                return {int(k): v for k, v in data.items()}
            
            elif path.suffix == ".json":
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"Loaded pixel mappings from: {path}")
                return {int(k): v for k, v in data.items()}
                
        except Exception as e:
            logger.warning(f"Failed to load {path}: {e}")
            continue
    
    logger.warning("No pixel mappings file found")
    return {}


def load_pixel_mappings(data_dir: Path, catchments_df: Optional[pd.DataFrame] = None) -> Dict[int, List[int]]:
    """
    Load pixel mappings using best available method.
    
    Tries in order:
    1. Extract from catchments DataFrame (new format)
    2. Load from pickle/JSON file (legacy format)
    
    Args:
        data_dir: Root data directory
        catchments_df: Optional catchments DataFrame with pixel_indices
        
    Returns:
        Dictionary mapping catchment_id to list of pixel indices
    """
    logger = logging.getLogger(__name__)
    
    # Method 1: Extract from catchments DataFrame
    if catchments_df is not None and not catchments_df.empty:
        mappings = load_pixel_mappings_from_catchments(catchments_df)
        if mappings:
            return mappings
    
    # Method 2: Load from file
    mappings = load_pixel_mappings_from_file(data_dir)
    if mappings:
        return mappings
    
    logger.warning("Could not load pixel mappings from any source")
    return {}


def analyze_catchment(
    radar_dir: Path,
    catchment_id: int,
    catchment_name: str,
    pixel_count: int,
) -> Dict[str, Any]:
    """
    Analyze radar data for one catchment.
    
    Args:
        radar_dir: Directory containing radar CSV files
        catchment_id: Catchment ID
        catchment_name: Catchment name
        pixel_count: Number of pixels in catchment
        
    Returns:
        Dictionary with analysis results
    """
    logger = logging.getLogger(__name__)
    
    # Find radar file for this catchment
    # Pattern: {catchment_id}_{catchment_name}.csv
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
    
    try:
        df = pd.read_csv(radar_files[0])
        
        # Handle both old format (value) and new format (value, weighted_value)
        value_col = "value"
        if "weighted_value" in df.columns:
            value_col = "weighted_value"
        
        pixel_stats = df.groupby("pixel_index").agg({
            value_col: ["sum", "max", "count"]
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
        
    except Exception as e:
        logger.warning(f"Failed to analyze catchment {catchment_id}: {e}")
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
            "error": str(e),
        }


def load_and_analyze(data_dir: Path) -> pd.DataFrame:
    """
    Load radar data and analyze all catchments.
    
    Compatible with both old and new RadarCollector output formats.
    
    Args:
        data_dir: Root data directory containing:
                  - catchments/ (JSON files) OR collection_summary.json
                  - radar_data/ (CSV files)
        
    Returns:
        DataFrame with analysis results for all catchments
        
    Raises:
        ValueError: If no catchments or radar data found
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Loading and analyzing radar data from: {data_dir}")
    
    # Determine radar_data directory location
    radar_dir = data_dir / "radar_data"
    if not radar_dir.exists():
        # Maybe data_dir IS the radar_data directory
        if list(data_dir.glob("*.csv")):
            radar_dir = data_dir
            data_dir = data_dir.parent
        else:
            raise ValueError(f"No radar data directory found in: {data_dir}")
    
    # Load catchments
    catchments = load_catchments(data_dir)
    
    # Load pixel mappings (try from catchments first, then from file)
    pixel_mappings = load_pixel_mappings(data_dir, catchments)
    
    # If no structured catchment data, build from radar files
    if catchments.empty or not pixel_mappings:
        logger.info("Building catchment list from radar CSV files...")
        radar_files = list(radar_dir.glob("*.csv"))
        
        if not radar_files:
            raise ValueError(f"No radar CSV files found in: {radar_dir}")
        
        # Build catchment info from filenames
        catchment_data = []
        for f in radar_files:
            parts = f.stem.split("_", 1)
            catchment_id = int(parts[0]) if parts[0].isdigit() else hash(f.stem) % 100000
            catchment_name = parts[1] if len(parts) > 1 else f.stem
            
            # Count pixels from file
            try:
                df = pd.read_csv(f)
                pixel_count = df["pixel_index"].nunique() if "pixel_index" in df.columns else 0
            except:
                pixel_count = 0
            
            catchment_data.append({
                "id": catchment_id,
                "name": catchment_name,
                "pixel_count": pixel_count,
            })
        
        catchments = pd.DataFrame(catchment_data)
        
        # Build pixel mappings
        pixel_mappings = {row["id"]: [] for _, row in catchments.iterrows()}
        
        logger.info(f"Built catchment list from {len(radar_files)} radar files")
    
    if catchments.empty:
        raise ValueError("No catchments found")
    
    logger.info(f"Analyzing {len(catchments)} catchments...")
    
    stats = []
    total = len(catchments)
    
    for idx, row in catchments.iterrows():
        catchment_id = row.get("id")
        catchment_name = row.get("name", f"ID_{catchment_id}")
        
        # Get pixel count from mappings or from DataFrame
        if catchment_id in pixel_mappings:
            pixel_count = len(pixel_mappings[catchment_id])
        elif "pixel_count" in row:
            pixel_count = row["pixel_count"]
        elif "pixel_indices" in row and row["pixel_indices"]:
            pixel_count = len(row["pixel_indices"])
        else:
            pixel_count = 0
        
        stat = analyze_catchment(radar_dir, catchment_id, catchment_name, pixel_count)
        stats.append(stat)
        
        if (idx + 1) % 50 == 0:
            logger.info(f"  Progress: {idx + 1}/{total}")
    
    logger.info(f"✓ Analyzed {len(stats)} catchments")
    return pd.DataFrame(stats)