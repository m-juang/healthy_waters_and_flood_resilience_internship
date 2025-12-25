from __future__ import annotations
from pathlib import Path
import pandas as pd
from moata_pipeline.common.dataframe_utils import ensure_columns, to_numeric_series, clean_text_series

EXPECTED_COLUMNS = [
    "Gauge",
    "Trace",
    "Alarm Name",
    "Type",
    "Threshold",
]


def classify_row(r: pd.Series) -> str:
    """Classify row based on Type column."""
    alarm_type = str(r.get("Type") or "").strip().lower()
    
    if alarm_type == "recency":
        return "Data freshness (recency)"
    if "overflow" in alarm_type:
        return "Threshold alarm (overflow)"
    return "Other"


def load_and_clean(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    df = ensure_columns(df, EXPECTED_COLUMNS)

    # Parse threshold as numeric
    df["threshold_num"] = to_numeric_series(df["Threshold"])

    # Normalize text fields
    for col in ["Gauge", "Trace", "Alarm Name", "Type"]:
        if col in df.columns:
            df[col] = clean_text_series(df[col])

    # Categorize
    df["row_category"] = df.apply(classify_row, axis=1)

    # Sort
    df = df.sort_values(
        by=["Gauge", "Trace", "row_category", "threshold_num"],
        ascending=[True, True, True, True],
        na_position="last",
    )

    return df