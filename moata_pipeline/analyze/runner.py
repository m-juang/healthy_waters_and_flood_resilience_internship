from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from moata_pipeline.common.json_io import read_json_maybe_wrapped, write_json
from moata_pipeline.common.paths import PipelinePaths

from .filtering import FilterConfig, filter_gauges
from .alarm_analysis import analyze_alarms
from .reporting import create_summary_report
from moata_pipeline.common.constants import INACTIVE_THRESHOLD_MONTHS, DEFAULT_EXCLUDE_KEYWORD

logger = logging.getLogger(__name__)


def run_filter_active_gauges(
    input_json: Optional[Path] = None,
    out_dir: Optional[Path] = None,
    inactive_months: int = INACTIVE_THRESHOLD_MONTHS,
    exclude_keyword: str = DEFAULT_EXCLUDE_KEYWORD,
) -> Dict[str, Any]:
    """
    Offline pipeline:
      1) Load outputs/rain_gauges/raw/rain_gauges_traces_alarms.json
      2) Filter active Auckland gauges
      3) Create alarm summary dataframes (all traces + alarms only)
      4) Save JSON/CSV/TXT artifacts

    Output files:
      - active_auckland_gauges.json: filtered gauge data
      - all_traces.csv: all traces (with or without alarms) - full detail
      - alarm_summary.csv: only alarms (recency + overflow) - simple view
      - alarm_summary_full.csv: only alarms - full detail
      - analysis_report.txt: text summary
    """
    paths = PipelinePaths()

    input_path: Path = input_json or paths.rain_gauges_traces_alarms_json
    output_dir: Path = out_dir or paths.rain_gauges_analyze_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Reading collect output: %s", input_path)
    print(f">>> Reading: {input_path}")
    
    all_data = read_json_maybe_wrapped(input_path)
    if not isinstance(all_data, list):
        raise ValueError(f"Expected list at {input_path}, got {type(all_data)}")

    cfg = FilterConfig(
        inactive_threshold_months=inactive_months,
        exclude_keyword=exclude_keyword,
    )

    filtered = filter_gauges(all_data, cfg)
    print(f">>> Filtered to {len(filtered.get('active_gauges', []))} active gauges")

    # Save active gauges JSON
    active_serializable = []
    for g in filtered.get("active_gauges", []):
        g2 = dict(g)
        g2.pop("last_data_time_dt", None)
        active_serializable.append(g2)

    write_json(output_dir / "active_auckland_gauges.json", active_serializable)

    # Get both DataFrames
    all_traces_df, alarms_only_df = analyze_alarms(filtered.get("active_gauges", []))

    # Save all traces CSV (full detail)
    if all_traces_df is not None and not all_traces_df.empty:
        all_traces_df.to_csv(output_dir / "all_traces.csv", index=False)
        print(f">>> Saved all_traces.csv: {len(all_traces_df)} rows")

    # Save alarms CSV
    if alarms_only_df is not None and not alarms_only_df.empty:
        # Full version (all columns)
        alarms_only_df.to_csv(output_dir / "alarm_summary_full.csv", index=False)
        print(f">>> Saved alarm_summary_full.csv: {len(alarms_only_df)} rows")

        # Simple version (essential columns only)
        simple_cols = [
            "gauge_name",
            "trace_description",
            "alarm_name",
            "alarm_type",
            "threshold",
        ]
        alarms_simple_df = alarms_only_df[simple_cols].copy()
        alarms_simple_df.columns = ["Gauge", "Trace", "Alarm Name", "Type", "Threshold"]
        alarms_simple_df.to_csv(output_dir / "alarm_summary.csv", index=False)
        print(f">>> Saved alarm_summary.csv: {len(alarms_simple_df)} rows (simple view)")

    # Generate report
    report = create_summary_report(
        filtered,
        alarms_only_df if alarms_only_df is not None else pd.DataFrame(),
    )
    (output_dir / "analysis_report.txt").write_text(report, encoding="utf-8")
    print(f">>> Saved analysis_report.txt")

    print(f">>> Done! Output dir: {output_dir}")

    return {
        "output_dir": output_dir,
        "filtered_data": filtered,
        "all_traces_df": all_traces_df,
        "alarms_only_df": alarms_only_df,
        "report": report,
    }