"""
DataFrame Utilities Module

Provides utility functions for DataFrame and Series type coercion, cleaning,
and validation.

Functions:
    coerce_bool_series: Convert Series to boolean with flexible input handling
    coerce_numeric_series: Convert Series to numeric type
    coerce_datetime_series: Convert Series to datetime type
    to_bool_series: Alias for coerce_bool_series
    to_numeric_series: Alias for coerce_numeric_series
    clean_text_series: Clean and normalize text Series
    ensure_columns: Ensure DataFrame has all required columns

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

from typing import List

import pandas as pd


# Version info
__version__ = "1.0.0"


# =============================================================================
# Type Coercion Functions
# =============================================================================

def coerce_bool_series(s: pd.Series) -> pd.Series:
    """
    Convert Series to boolean with flexible input handling.
    
    Handles multiple input formats:
        - Boolean: True/False (returned as-is)
        - String: "true"/"false", "yes"/"no", "1"/"0" (case-insensitive)
        - Numeric: 1/0
        - Invalid values: Converted to False
        
    Args:
        s: Input Series to convert
        
    Returns:
        Boolean Series with all values coerced to True/False
        
    Example:
        >>> data = pd.Series(["true", "False", "yes", "0", None, "invalid"])
        >>> result = coerce_bool_series(data)
        >>> print(result.tolist())
        [True, False, True, False, False, False]
    """
    if not isinstance(s, pd.Series):
        raise TypeError(f"Expected pd.Series, got {type(s).__name__}")
    
    # Already boolean - return as-is
    if s.dtype == bool:
        return s
    
    # Convert to string and map common boolean representations
    mapped = (
        s.astype(str)
        .str.strip()
        .str.lower()
        .map({
            "true": True,
            "false": False,
            "1": True,
            "0": False,
            "yes": True,
            "no": False,
            "y": True,
            "n": False,
            "t": True,
            "f": False,
        })
    )
    
    # Fill unmapped values with False
    return mapped.fillna(False).astype(bool)


def coerce_numeric_series(s: pd.Series) -> pd.Series:
    """
    Convert Series to numeric type with error handling.
    
    Invalid values are converted to NaN.
    
    Args:
        s: Input Series to convert
        
    Returns:
        Numeric Series (int or float) with invalid values as NaN
        
    Example:
        >>> data = pd.Series(["123", "45.6", "invalid", None])
        >>> result = coerce_numeric_series(data)
        >>> print(result.tolist())
        [123.0, 45.6, nan, nan]
    """
    if not isinstance(s, pd.Series):
        raise TypeError(f"Expected pd.Series, got {type(s).__name__}")
    
    return pd.to_numeric(s, errors="coerce")


def coerce_datetime_series(s: pd.Series) -> pd.Series:
    """
    Convert Series to datetime type with error handling.
    
    Invalid values are converted to NaT (Not a Time).
    
    Args:
        s: Input Series to convert
        
    Returns:
        Datetime Series with invalid values as NaT
        
    Example:
        >>> data = pd.Series(["2024-12-28", "2024-01-01", "invalid", None])
        >>> result = coerce_datetime_series(data)
        >>> print(result.tolist())
        [Timestamp('2024-12-28'), Timestamp('2024-01-01'), NaT, NaT]
    """
    if not isinstance(s, pd.Series):
        raise TypeError(f"Expected pd.Series, got {type(s).__name__}")
    
    return pd.to_datetime(s, errors="coerce")


# =============================================================================
# Alias Functions (for backward compatibility)
# =============================================================================

def to_bool_series(s: pd.Series) -> pd.Series:
    """
    Alias for coerce_bool_series.
    
    Used by viz/cleaning.py for backward compatibility.
    
    Args:
        s: Input Series to convert
        
    Returns:
        Boolean Series
    """
    return coerce_bool_series(s)


def to_numeric_series(s: pd.Series) -> pd.Series:
    """
    Alias for coerce_numeric_series.
    
    Used by viz/cleaning.py for backward compatibility.
    
    Args:
        s: Input Series to convert
        
    Returns:
        Numeric Series
    """
    return coerce_numeric_series(s)


# =============================================================================
# Cleaning Functions
# =============================================================================

def clean_text_series(s: pd.Series) -> pd.Series:
    """
    Clean text Series by converting to string and stripping whitespace.
    
    NaN/None values are converted to empty strings.
    
    Args:
        s: Input Series to clean
        
    Returns:
        Series with cleaned text (no leading/trailing whitespace)
        
    Example:
        >>> data = pd.Series(["  hello  ", "world", None, 123])
        >>> result = clean_text_series(data)
        >>> print(result.tolist())
        ['hello', 'world', '', '123']
    """
    if not isinstance(s, pd.Series):
        raise TypeError(f"Expected pd.Series, got {type(s).__name__}")
    
    return s.fillna("").astype(str).str.strip()


# =============================================================================
# DataFrame Validation Functions
# =============================================================================

def ensure_columns(
    df: pd.DataFrame,
    expected_columns: List[str],
    fill_value: any = None,
) -> pd.DataFrame:
    """
    Ensure DataFrame has all expected columns.
    
    Missing columns will be added with specified fill value.
    Returns a copy of the DataFrame to avoid modifying the original.
    
    Args:
        df: Input DataFrame
        expected_columns: List of column names that should exist
        fill_value: Value to use for missing columns (default: None)
        
    Returns:
        DataFrame with all expected columns (copy of original)
        
    Raises:
        TypeError: If df is not a DataFrame
        ValueError: If expected_columns is empty
        
    Example:
        >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        >>> result = ensure_columns(df, ["a", "b", "c", "d"])
        >>> print(result.columns.tolist())
        ['a', 'b', 'c', 'd']
        >>> print(result['c'].tolist())
        [None, None]
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pd.DataFrame, got {type(df).__name__}")
    
    if not expected_columns:
        raise ValueError("expected_columns cannot be empty")
    
    # Create a copy to avoid modifying original
    df = df.copy()
    
    # Add missing columns
    for col in expected_columns:
        if col not in df.columns:
            df[col] = fill_value
    
    return df


def validate_columns_exist(
    df: pd.DataFrame,
    required_columns: List[str],
) -> None:
    """
    Validate that DataFrame contains all required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Raises:
        TypeError: If df is not a DataFrame
        ValueError: If any required columns are missing
        
    Example:
        >>> df = pd.DataFrame({"a": [1], "b": [2]})
        >>> validate_columns_exist(df, ["a", "b"])  # OK
        >>> validate_columns_exist(df, ["a", "c"])  # Raises ValueError
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pd.DataFrame, got {type(df).__name__}")
    
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        raise ValueError(
            f"DataFrame missing required columns: {missing}\n"
            f"Available columns: {df.columns.tolist()}"
        )


def drop_empty_rows(
    df: pd.DataFrame,
    subset: List[str] = None,
) -> pd.DataFrame:
    """
    Drop rows where all values in subset columns are NaN/empty.
    
    Args:
        df: Input DataFrame
        subset: Columns to check (default: all columns)
        
    Returns:
        DataFrame with empty rows removed
        
    Example:
        >>> df = pd.DataFrame({
        ...     "a": [1, None, None],
        ...     "b": [2, None, 3]
        ... })
        >>> result = drop_empty_rows(df)
        >>> len(result)
        2
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pd.DataFrame, got {type(df).__name__}")
    
    return df.dropna(how="all", subset=subset)


def drop_duplicate_rows(
    df: pd.DataFrame,
    subset: List[str] = None,
    keep: str = "first",
) -> pd.DataFrame:
    """
    Drop duplicate rows based on subset of columns.
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for duplicates (default: all)
        keep: Which duplicates to keep ('first', 'last', False)
        
    Returns:
        DataFrame with duplicates removed
        
    Example:
        >>> df = pd.DataFrame({
        ...     "a": [1, 1, 2],
        ...     "b": [2, 2, 3]
        ... })
        >>> result = drop_duplicate_rows(df, subset=["a"])
        >>> len(result)
        2
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pd.DataFrame, got {type(df).__name__}")
    
    if keep not in ["first", "last", False]:
        raise ValueError(f"keep must be 'first', 'last', or False, got {keep}")
    
    return df.drop_duplicates(subset=subset, keep=keep)