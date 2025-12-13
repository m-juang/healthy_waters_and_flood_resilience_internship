from __future__ import annotations

import pandas as pd


def coerce_bool_series(s: pd.Series) -> pd.Series:
    """
    Handles True/False, "True"/"False", 1/0, yes/no.
    """
    if s.dtype == bool:
        return s
    mapped = (
        s.astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
    )
    return mapped.fillna(False).astype(bool)


def coerce_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def coerce_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def to_bool_series(s: pd.Series) -> pd.Series:
    """
    Alias untuk coerce_bool_series (digunakan oleh viz/cleaning.py)
    """
    return coerce_bool_series(s)


def to_numeric_series(s: pd.Series) -> pd.Series:
    """
    Alias untuk coerce_numeric_series (digunakan oleh viz/cleaning.py)
    """
    return coerce_numeric_series(s)


def clean_text_series(s: pd.Series) -> pd.Series:
    """
    Clean text series: convert to string and strip whitespace.
    Returns empty string for NaN values.
    """
    return s.fillna("").astype(str).str.strip()


def ensure_columns(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    """
    Ensure dataframe has all expected columns.
    Missing columns will be added with None values.
    
    Args:
        df: Input dataframe
        expected_columns: List of column names that should exist
    
    Returns:
        DataFrame with all expected columns
    """
    df = df.copy()
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    return df