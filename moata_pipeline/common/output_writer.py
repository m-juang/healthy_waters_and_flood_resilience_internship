from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List


class JsonOutputWriter:
    def __init__(self, out_dir: Path) -> None:
        self._out_dir = out_dir

    def ensure_dir(self) -> None:
        self._out_dir.mkdir(parents=True, exist_ok=True)

    def write_json(self, filename: str, data: Any) -> Path:
        self.ensure_dir()
        path = self._out_dir / filename
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return path

    def write_rain_gauges(self, gauges: List[Dict[str, Any]]) -> Path:
        return self.write_json("rain_gauges.json", gauges)

    def write_combined(self, all_data: List[Dict[str, Any]]) -> Path:
        return self.write_json("rain_gauges_traces_alarms.json", all_data)
