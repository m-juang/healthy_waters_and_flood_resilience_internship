from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class PipelinePaths:
    """
    Central place to keep filesystem paths stable.
    Changing folder names only needs updating here.
    """
    output_dir: Path = Path("moata_output")
    filtered_dir: Path = Path("moata_filtered")
    viz_dir: Path = Path("moata_filtered") / "viz"

    # Common file names (optional convenience)
    gauges_json: Path = output_dir / "rain_gauges.json"
    all_data_json: Path = output_dir / "rain_gauges_traces_alarms.json"
    detailed_alarms_json: Path = output_dir / "detailed_alarms.json"

    active_gauges_json: Path = filtered_dir / "active_auckland_gauges.json"
    alarm_summary_csv: Path = filtered_dir / "alarm_summary.csv"
    alarm_summary_json: Path = filtered_dir / "alarm_summary.json"
    analysis_report_txt: Path = filtered_dir / "analysis_report.txt"

    viz_report_html: Path = viz_dir / "report.html"
    viz_cleaned_csv: Path = viz_dir / "cleaned_alarm_summary.csv"
