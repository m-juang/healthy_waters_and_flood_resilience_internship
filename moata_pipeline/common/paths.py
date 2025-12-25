from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    """
    Canonical directory structure for pipeline outputs.
    
    outputs/
    ├── rain_gauges/
    │   ├── raw/
    │   ├── analyze/
    │   └── visualizations/
    └── rain_radar/
        ├── raw/
        └── visualizations/
    """
    # Root directory for all outputs
    outputs_root: Path = Path("outputs")

    # -------------------------
    # Rain gauges
    # -------------------------
    @property
    def rain_gauges_dir(self) -> Path:
        return self.outputs_root / "rain_gauges"

    @property
    def rain_gauges_raw_dir(self) -> Path:
        return self.rain_gauges_dir / "raw"

    @property
    def rain_gauges_analyze_dir(self) -> Path:
        return self.rain_gauges_dir / "analyze"

    @property
    def rain_gauges_filtered_dir(self) -> Path:
        """Deprecated: use rain_gauges_analyze_dir instead."""
        return self.rain_gauges_analyze_dir

    @property
    def rain_gauges_viz_dir(self) -> Path:
        return self.rain_gauges_dir / "visualizations"

    # -------------------------
    # Rain radar
    # -------------------------
    @property
    def rain_radar_dir(self) -> Path:
        return self.outputs_root / "rain_radar"

    @property
    def rain_radar_raw_dir(self) -> Path:
        return self.rain_radar_dir / "raw"

    @property
    def rain_radar_viz_dir(self) -> Path:
        return self.rain_radar_dir / "visualizations"

    # -------------------------
    # General alias
    # -------------------------
    @property
    def viz_dir(self) -> Path:
        """Alias for rain_gauges_viz_dir (default visualization output)."""
        return self.rain_gauges_viz_dir

    # -------------------------
    # Canonical file paths
    # -------------------------
    # Rain gauges - raw
    @property
    def rain_gauges_traces_alarms_json(self) -> Path:
        return self.rain_gauges_raw_dir / "rain_gauges_traces_alarms.json"

    @property
    def rain_gauges_json(self) -> Path:
        return self.rain_gauges_raw_dir / "rain_gauges.json"

    # Rain gauges - analyze
    @property
    def alarm_summary_csv(self) -> Path:
        return self.rain_gauges_analyze_dir / "alarm_summary.csv"

    @property
    def all_traces_csv(self) -> Path:
        return self.rain_gauges_analyze_dir / "all_traces.csv"

    # Rain radar (example placeholders; adjust filenames as needed)
    @property
    def rain_radar_catchments_json(self) -> Path:
        return self.rain_radar_raw_dir / "catchments.json"

    @property
    def rain_radar_pixels_json(self) -> Path:
        return self.rain_radar_raw_dir / "pixels.json"

    @property
    def rain_radar_qpe_sample_json(self) -> Path:
        return self.rain_radar_raw_dir / "qpe_sample.json"