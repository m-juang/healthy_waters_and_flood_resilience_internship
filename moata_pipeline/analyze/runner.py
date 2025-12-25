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
      1) Load moata_output/rain_gauges_traces_alarms.json
      2) Filter active Auckland gauges
      3) Create alarm summary dataframe
      4) Save JSON/CSV/TXT artifacts

    Returns a dict with filtered_data + alarms_df.
    """
    paths = PipelinePaths()
    input_path = input_json or paths.all_data_json
    output_dir = out_dir or paths.filtered_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    all_data = read_json_maybe_wrapped(input_path)
    if not isinstance(all_data, list):
        raise ValueError(f"Expected list at {input_path}, got {type(all_data)}")

    cfg = FilterConfig(inactive_threshold_months=inactive_months, exclude_keyword=exclude_keyword)
    filtered = filter_gauges(all_data, cfg)

    # Save active gauges JSON (remove non-serializable dt field)
    active_serializable = []
    for g in filtered["active_gauges"]:
        g2 = dict(g)
        if "last_data_time_dt" in g2:
            del g2["last_data_time_dt"]
        active_serializable.append(g2)

    write_json(output_dir / "active_auckland_gauges.json", active_serializable)

    alarms_df = analyze_alarms(filtered["active_gauges"])
    if alarms_df is not None and not alarms_df.empty:
        alarms_df.to_csv(output_dir / "alarm_summary.csv", index=False)
        alarms_df.to_json(output_dir / "alarm_summary.json", orient="records", indent=2)

    report = create_summary_report(filtered, alarms_df if alarms_df is not None else pd.DataFrame())
    (output_dir / "analysis_report.txt").write_text(report, encoding="utf-8")

    return {
        "output_dir": output_dir,
        "filtered_data": filtered,
        "alarms_df": alarms_df,
        "report": report,
    }
