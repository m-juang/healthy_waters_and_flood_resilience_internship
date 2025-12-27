"""
Visualization Data Cleaning Module

Provides data cleaning and preparation functions for visualization reports.

Functions:
    load_and_clean: Load and clean alarm summary CSV
    classify_row: Classify alarm row by type
    validate_dataframe: Validate DataFrame structure

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from moata_pipeline.common.dataframe_utils import (
    ensure_columns,
    to_numeric_series,
    clean_text_series,
)


# Version info
__version__ = "1.0.0"


# Expected CSV columns
EXPECTED_COLUMNS = [
    "Gauge",
    "Trace",
    "Alarm Name",
    "Type",
    "Threshold",
]


# =============================================================================
# Classification Functions
# =============================================================================

def classify_row(row: pd.Series) -> str:
    """
    Classify alarm row based on Type column.
    
    Categories:
        - Data freshness (recency)
        - Threshold alarm (overflow)
        - Other
        
    Args:
        row: DataFrame row as Series
        
    Returns:
        Classification string
        
    Example:
        >>> row = pd.Series({"Type": "Recency"})
        >>> classify_row(row)
        'Data freshness (recency)'
    """
    alarm_type = str(row.get("Type") or "").strip().lower()
    
    if alarm_type == "recency":
        return "Data freshness (recency)"
    
    if "overflow" in alarm_type:
        return "Threshold alarm (overflow)"
    
    return "Other"


# =============================================================================
# Validation Functions
# =============================================================================

def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate DataFrame has required structure.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        ValueError: If DataFrame is invalid
    """
    if df.empty:
        raise ValueError("DataFrame is empty - no data to visualize")
    
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"DataFrame missing required columns: {missing_cols}\n"
            f"Available columns: {df.columns.tolist()}"
        )


# =============================================================================
# Main Cleaning Function
# =============================================================================

def load_and_clean(csv_path: Path) -> pd.DataFrame:
    """
    Load and clean alarm summary CSV for visualization.
    
    Processing Steps:
        1. Load CSV file
        2. Clean column names (strip whitespace)
        3. Ensure required columns exist
        4. Parse threshold as numeric
        5. Normalize text fields
        6. Categorize rows
        7. Sort by gauge, trace, category, threshold
        
    Args:
        csv_path: Path to alarm summary CSV file
        
    Returns:
        Cleaned DataFrame ready for visualization
        
    Raises:
        FileNotFoundError: If CSV file not found
        ValueError: If CSV is empty or invalid
        
    Example:
        >>> df = load_and_clean(Path("outputs/rain_gauges/analyze/alarm_summary.csv"))
        >>> print(df.columns.tolist())
        ['Gauge', 'Trace', 'Alarm Name', 'Type', 'Threshold', ...]
    """
    if not isinstance(csv_path, Path):
        csv_path = Path(csv_path)
    
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Alarm summary CSV not found: {csv_path}\n\n"
            f"Run analysis first:\n"
            f"  python analyze_rain_gauges.py"
        )
    
    # Load CSV
    try:
        df = pd.read_csv(csv_path)
    except pd.errors.EmptyDataError:
        raise ValueError(f"CSV file is empty: {csv_path}")
    except Exception as e:
        raise ValueError(f"Failed to load CSV {csv_path}: {e}") from e
    
    # Clean column names
    df.columns = [col.strip() for col in df.columns]
    
    # Ensure required columns exist
    df = ensure_columns(df, EXPECTED_COLUMNS)
    
    # Validate
    validate_dataframe(df)
    
    # Parse threshold as numeric
    df["threshold_num"] = to_numeric_series(df["Threshold"])
    
    # Normalize text fields
    text_columns = ["Gauge", "Trace", "Alarm Name", "Type"]
    for col in text_columns:
        if col in df.columns:
            df[col] = clean_text_series(df[col])
    
    # Categorize rows
    df["row_category"] = df.apply(classify_row, axis=1)
    
    # Sort for consistent display
    sort_columns = ["Gauge", "Trace", "row_category", "threshold_num"]
    df = df.sort_values(
        by=sort_columns,
        ascending=[True, True, True, True],
        na_position="last",
    )
    
    return df