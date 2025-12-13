from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class PipelinePaths:
    root: Path = Path(".")
    output_dir: Path = Path("moata_output")
    filtered_dir: Path = Path("moata_filtered")
    viz_dir: Path = Path("moata_filtered") / "viz"

    # --- canonical paths ---
    @property
    def rain_gauges_traces_alarms_json(self) -> Path:
        return self.output_dir / "rain_gauges_traces_alarms.json"

    @property
    def rain_gauges_json(self) -> Path:
        return self.output_dir / "rain_gauges.json"

    # --- backward-compat aliases (so older runner code still works) ---
    @property
    def all_data_json(self) -> Path:
        # what analyze/runner.py expects
        return self.rain_gauges_traces_alarms_json

    @property
    def alarm_summary_csv(self) -> Path:
        return self.filtered_dir / "alarm_summary.csv"
