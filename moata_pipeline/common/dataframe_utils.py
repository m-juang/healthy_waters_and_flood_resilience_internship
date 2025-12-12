from __future__ import annotations
import pandas as pd

def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Make sure dataframe has all columns (create missing with NA).
    Returns df for chaining.
    """
    for c in columns:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def to_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def to_bool_series(s: pd.Series) -> pd.Series:
    """
    Handles True/False, "True"/"False", 1/0, yes/no
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

def clean_text_series(s: pd.Series) -> pd.Series:
    """
    Normalize text columns: strip, handle nan.
    """
    return s.astype(str).replace({"nan": ""}).fillna("").str.strip()
