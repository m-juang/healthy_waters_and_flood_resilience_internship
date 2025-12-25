from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from moata_pipeline.moata.client import MoataClient

logger = logging.getLogger(__name__)


class RainGaugeCollector:
    def __init__(self, client: MoataClient) -> None:
        self._client = client

    @staticmethod
    def _chunk(items: List[int], size: int) -> List[List[int]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    @staticmethod
    def _safe_int(x: Any) -> int | None:
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    def collect(
        self,
        project_id: int,
        asset_type_id: int,
        trace_batch_size: int = 100,
        fetch_thresholds: bool = True,
    ) -> List[Dict[str, Any]]:
        # 1) Assets
        gauges = self._client.get_rain_gauges(project_id=project_id, asset_type_id=asset_type_id)
        logger.info("✓ Fetched %d rain gauges", len(gauges))

        # 2) Project-level detailed alarms (map by traceId)
        detailed_by_trace = self._client.get_detailed_alarms_by_project(project_id=project_id)
        logger.info("✓ Fetched %d detailed alarms (project-level)", len(detailed_by_trace))

        # Gather asset ids
        asset_ids: List[int] = []
        gauge_by_asset_id: Dict[int, Dict[str, Any]] = {}

        for g in gauges:
            asset_id = g.get("id") or g.get("assetId")
            asset_id_int = self._safe_int(asset_id)
            if asset_id_int is None:
                logger.warning("Gauge without valid id: %s", g)
                continue
            asset_ids.append(asset_id_int)
            gauge_by_asset_id[asset_id_int] = g

        if not asset_ids:
            return []

        # 3) Traces (BATCH, swagger-friendly assetId=array)
        all_traces: List[Dict[str, Any]] = []
        for batch in self._chunk(asset_ids, trace_batch_size):
            traces = self._client.get_traces_for_assets(batch)
            all_traces.extend(traces)

        logger.info("✓ Fetched %d traces (batched)", len(all_traces))

        # Group traces by assetId
        traces_by_asset: Dict[int, List[Dict[str, Any]]] = {}
        for t in all_traces:
            aid = self._safe_int(t.get("assetId"))
            if aid is None:
                continue
            traces_by_asset.setdefault(aid, []).append(t)

        # 4) For each gauge/asset, enrich traces with alarms/thresholds
        all_data: List[Dict[str, Any]] = []

        for idx, asset_id in enumerate(asset_ids, start=1):
            g = gauge_by_asset_id.get(asset_id, {})
            name = g.get("name", "Unknown")

            logger.info("Processing %d/%d: %s (asset_id=%s)", idx, len(asset_ids), name, asset_id)

            traces = traces_by_asset.get(asset_id, [])
            traces_out: List[Dict[str, Any]] = []

            for t in traces:
                trace_id = t.get("id") or t.get("traceId")
                trace_id_int = self._safe_int(trace_id)
                if trace_id_int is None:
                    continue

                # Swagger: hasAlarms indicates whether trace has any alarms assigned
                has_alarms = bool(t.get("hasAlarms", False))

                alarms_raw: List[Dict[str, Any]] = []
                alarms_split: Dict[str, List[Dict[str, Any]]] = {"overflow": [], "recency": [], "other": []}
                thresholds: List[Dict[str, Any]] = []

                if has_alarms:
                    alarms_raw = self._client.get_alarms_for_trace(trace_id_int)
                    alarms_split = self._client.split_alarms_by_type(alarms_raw)

                    if fetch_thresholds:
                        thresholds = self._client.get_thresholds_for_trace(trace_id_int)

                detailed_alarm = detailed_by_trace.get(trace_id_int)

                traces_out.append(
                    {
                        "trace": t,
                        "alarms": alarms_raw,                 # raw list
                        "alarms_by_type": alarms_split,       # split overflow/recency
                        "detailed_alarm": detailed_alarm,     # project-level map
                        "thresholds": thresholds,             # optional
                    }
                )

            all_data.append({"gauge": g, "traces": traces_out})

        return all_data
