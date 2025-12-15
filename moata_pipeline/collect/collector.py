from __future__ import annotations
import logging
from typing import Any, Dict, List
from moata_pipeline.moata.client import MoataClient


class RainGaugeCollector:
    def __init__(self, client: MoataClient) -> None:
        self._client = client

    def collect(self, project_id: int, asset_type_id: int) -> List[Dict[str, Any]]:
        gauges = self._client.get_rain_gauges(project_id=project_id, asset_type_id=asset_type_id)
        logging.info("✓ Fetched %d rain gauges", len(gauges))

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

        return all_data
