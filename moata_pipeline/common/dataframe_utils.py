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
