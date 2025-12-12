from __future__ import annotations

from pathlib import Path
import pandas as pd

from moata_pipeline.common.dataframe_utils import ensure_columns, to_bool_series, to_numeric_series, clean_text_series


EXPECTED_COLUMNS = [
    "gauge_id",
    "gauge_name",
    "last_data",
    "trace_id",
    "trace_name",
    "alarm_id",
    "alarm_name",
    "alarm_type",
    "threshold",
    "severity",
    "is_critical",
    "source",
]


def parse_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def classify_row(r: pd.Series) -> str:
    src = (r.get("source") or "").lower()
    at = (r.get("alarm_type") or "").lower()
    alarm_id = r.get("alarm_id")
    has_alarm_id = pd.notna(alarm_id) and str(alarm_id).strip() != ""

    if "has_alarms" in src or at == "datarecency":
        return "Data freshness (recency)"
    if "threshold" in src or at == "overflow":
        return "Threshold alarm (overflow)"
    if has_alarm_id:
        return "Alarm record"
    return "Other"


def load_and_clean(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    df = ensure_columns(df, EXPECTED_COLUMNS)

    # Types
    df["gauge_id"] = pd.to_numeric(df["gauge_id"], errors="coerce").astype("Int64")
    df["trace_id"] = pd.to_numeric(df["trace_id"], errors="coerce").astype("Int64")

    df["last_data_dt"] = parse_date_series(df["last_data"])
    df["threshold_num"] = to_numeric_series(df["threshold"])
    df["is_critical_bool"] = to_bool_series(df["is_critical"])

    # Normalize text fields
    for col in ["gauge_name", "trace_name", "alarm_name", "alarm_type", "severity", "source"]:
        df[col] = clean_text_series(df[col])

    # Categorize
    df["row_category"] = df.apply(classify_row, axis=1)

    # Nice sort
    df = df.sort_values(
        by=["gauge_name", "trace_name", "row_category", "threshold_num"],
        ascending=[True, True, True, True],
        na_position="last",
    )
    return df
