from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List
from moata_pipeline.moata.client import MoataClient


class RainGaugeCollector:
    def __init__(self, client: MoataClient, out_dir: Path) -> None:
        self._client = client
        self._out_dir = out_dir

    def collect(self, project_id: int, asset_type_id: int = 100) -> List[Dict[str, Any]]:
        self._out_dir.mkdir(parents=True, exist_ok=True)

        gauges = self._client.get_rain_gauges(project_id=project_id, asset_type_id=asset_type_id)
        (self._out_dir / "rain_gauges.json").write_text(json.dumps(gauges, indent=2), encoding="utf-8")
        logging.info("✓ Saved %d rain gauges", len(gauges))

        detailed = self._client.get_detailed_alarms_by_project(project_id=project_id)
        logging.info("✓ Fetched %d detailed alarms", len(detailed))

        all_data: List[Dict[str, Any]] = []
        for idx, g in enumerate(gauges, start=1):
            asset_id = g.get("id") or g.get("assetId")
            name = g.get("name", "Unknown")
            if asset_id is None:
                logging.warning("Gauge without id: %s", g)
                continue

            logging.info("Processing %d/%d: %s (asset_id=%s)", idx, len(gauges), name, asset_id)
            traces = self._client.get_traces_for_asset(asset_id)

            traces_out = []
            for t in traces:
                trace_id = t.get("id") or t.get("traceId")
                if trace_id is None:
                    continue

                alarms = self._client.get_overflow_alarms_for_trace(trace_id)
                thresholds = self._client.get_thresholds_for_trace(trace_id)
                detailed_alarm = detailed.get(int(trace_id)) if str(trace_id).isdigit() else detailed.get(trace_id)

                traces_out.append(
                    {
                        "trace": t,
                        "overflow_alarms": alarms,
                        "detailed_alarm": detailed_alarm,
                        "thresholds": thresholds,
                    }
                )

            all_data.append({"gauge": g, "traces": traces_out})

        (self._out_dir / "rain_gauges_traces_alarms.json").write_text(
            json.dumps(all_data, indent=2),
            encoding="utf-8",
        )
        logging.info("✓ Saved combined structure: %s", self._out_dir / "rain_gauges_traces_alarms.json")
        return all_data
