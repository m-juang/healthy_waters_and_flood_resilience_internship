"""
Rain gauge collector facade (orchestrates specialized collectors).

This module provides backwards compatibility with the old RainGaugeCollector
while using the new specialized collector architecture underneath.
"""
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging

from moata_pipeline.moata.client import MoataClient
from moata_pipeline.common.typing_utils import safe_int
from moata_pipeline.common.time_utils import iso_z

from .asset_fetcher import AssetFetcher
from .trace_fetcher import TraceFetcher
from .alarm_fetcher import AlarmFetcher
from .output_manager import OutputManager
from .rainfall_trace_filter import RainfallTraceFilter, FilterCriteria, FilterResult


class CollectionError(Exception):
    """Base exception for collection errors."""
    pass


def _ensure_aware_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class GaugeCollector:
    """
    Rain gauge data collector (Facade pattern).
    
    Orchestrates specialized collectors to fetch rain gauge data including
    traces, alarms, and thresholds. Maintains backwards compatibility with
    the original RainGaugeCollector interface.
    
    Architecture:
    - AssetFetcher: Fetch and prepare gauge assets
    - TraceFetcher: Fetch traces and timeseries data
    - AlarmFetcher: Fetch alarms and thresholds
    - OutputManager: Handle file I/O with atomic writes
    
    Single Responsibility: Orchestrate the collection workflow
    
    Example:
        >>> from moata_pipeline.moata.client import MoataClient
        >>> client = MoataClient(base_url, username, password)
        >>> collector = GaugeCollector(client)
        >>> data = collector.collect(project_id=594, asset_type_id=100)
        >>> print(f"Collected {len(data)} gauges")
    """
    
    def __init__(
        self,
        client: MoataClient,
        output_dir: Optional[Path] = None,
        enable_prefilter: bool = True,
    ) -> None:
        """
        Initialize gauge collector.
        
        Args:
            client: Authenticated MoataClient instance (or compatible HTTP client)
            output_dir: Optional output directory for atomic writes
            enable_prefilter: Enable pre-filtering inactive gauges (Sam's optimization)
        """
        # Initialize specialized collectors (accept any compatible client)
        self.assets = AssetFetcher(client)
        self.traces = TraceFetcher(client)
        self.alarms = AlarmFetcher(client)
        self.output = OutputManager(client, base_dir=output_dir, enable_atomic=True)
        
        # Pre-filter for inactive gauge optimization
        self._enable_prefilter = enable_prefilter
        self._prefilter = RainfallTraceFilter(client.traces) if enable_prefilter else None
        self._logger = logging.getLogger(f"{__name__}.GaugeCollector")
        
        # Keep reference to client for backwards compatibility
        self._client = client
    
    def collect(
        self,
        project_id: int,
        asset_type_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        trace_batch_size: int = 100,
        fetch_thresholds: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Collect complete rain gauge data.
        
        Fetches gauges, traces, alarms, and thresholds in an orchestrated workflow.
        
        Args:
            project_id: Moata project ID
            asset_type_id: Asset type ID for rain gauges
            start_time: Start of time range (default: 24 hours ago)
            end_time: End of time range (default: now)
            trace_batch_size: Number of assets to fetch traces for per batch
            fetch_thresholds: Whether to fetch alarm thresholds
            
        Returns:
            List of dictionaries, each containing:
                - gauge: Asset information
                - traces: List of trace data with alarms and thresholds
                
        Raises:
            ValueError: If parameters are invalid
            CollectionError: If collection fails
            
        Example:
            >>> data = collector.collect(
            ...     project_id=594,
            ...     asset_type_id=100,
            ...     start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            ...     end_time=datetime(2025, 1, 2, tzinfo=timezone.utc)
            ... )
        """
        # Set default time range
        if end_time is None:
            end_time = datetime.now(timezone.utc)
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
        
        # Ensure timezone-aware UTC
        start_time = _ensure_aware_utc(start_time)
        end_time = _ensure_aware_utc(end_time)
        
        # Setup atomic write temp directory if configured
        if self.output._enable_atomic:
            self.output.setup_temp_dir()
        
        try:
            # Step 1: Fetch assets
            gauges = self.assets.fetch_gauges(project_id, asset_type_id)
            
            # Step 2: Prepare asset lookup
            all_asset_ids, gauge_by_id = self.assets.prepare_asset_lookup(gauges)
            
            if not all_asset_ids:
                self.output._logger.warning("No valid asset IDs found")
                return []
            
            # Step 2.5: Pre-filter inactive gauges (Sam's optimization)
            asset_ids = all_asset_ids
            filter_result: Optional[FilterResult] = None
            
            if self._enable_prefilter and self._prefilter:
                try:
                    # Build asset name lookup for exclusion filtering
                    asset_names = {
                        aid: gauge_by_id.get(aid, {}).get("name", "")
                        for aid in all_asset_ids
                    }
                    
                    filter_result = self._prefilter.filter_active_gauges(
                        project_id=project_id,
                        asset_names=asset_names
                    )
                    
                    # Only process active gauges (significant API call reduction)
                    asset_ids = list(filter_result.active_asset_ids)
                    
                    self._logger.info(
                        f"Pre-filter: {len(asset_ids)}/{len(all_asset_ids)} active gauges "
                        f"(skipping {filter_result.inactive_count} inactive, "
                        f"{filter_result.excluded_count} excluded)"
                    )
                except Exception as e:
                    # Fall back to all assets if pre-filter fails
                    self._logger.warning(f"Pre-filter failed, processing all gauges: {e}")
                    asset_ids = all_asset_ids
            
            # Step 3: Fetch project-level alarms
            detailed_by_trace = self.alarms.fetch_detailed_alarms_by_project(project_id)
            
            # Step 4: Fetch traces in batches (only for active gauges)
            traces_by_asset = self.traces.fetch_traces_batched(
                asset_ids,
                batch_size=trace_batch_size,
                asset_names=gauge_by_id  # Pass names for better logging
            )
            
            # Step 5: Enrich gauges with trace data
            all_data = self._enrich_gauges_with_traces(
                asset_ids=asset_ids,
                gauge_by_id=gauge_by_id,
                traces_by_asset=traces_by_asset,
                detailed_by_trace=detailed_by_trace,
                fetch_thresholds=fetch_thresholds,
                start_time=start_time,
                end_time=end_time,
            )
            
            self.output._logger.info(f"✓ Collection complete: {len(all_data)} gauges")
            
            # Step 6: Write output if configured
            if self.output._enable_atomic and self.output._temp_dir:
                self.output.write_json(all_data, "rain_gauges_traces_alarms.json")
                self.output.finalize_output()
            
            return all_data
            
        except Exception as e:
            self.output._logger.error(f"Collection failed: {e}")
            raise CollectionError(f"Failed to collect rain gauge data: {e}") from e
    
    def _enrich_gauges_with_traces(
        self,
        asset_ids: List[int],
        gauge_by_id: Dict[int, Dict[str, Any]],
        traces_by_asset: Dict[int, List[Dict[str, Any]]],
        detailed_by_trace: Dict[int, Dict[str, Any]],
        fetch_thresholds: bool,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Enrich gauge data with traces, alarms, and timeseries.
        
        This is the core enrichment logic that combines data from multiple sources.
        """
        total_gauges = len(asset_ids)
        self.output._logger.info(f"Enriching gauges with trace data... (0/{total_gauges})")
        
        all_data: List[Dict[str, Any]] = []
        
        for idx, asset_id in enumerate(asset_ids, 1):
            gauge = gauge_by_id.get(asset_id)
            if gauge is None:
                continue
            
            # Try to get gauge name, fetch from API if not available
            gauge_name = gauge.get("name", "")
            if not gauge_name:
                # Use GET /v1/assets/{assetId} to get the name
                try:
                    asset_info = self._client.get_asset_by_id(asset_id)
                    if asset_info and asset_info.get("name"):
                        gauge_name = asset_info["name"]
                        # Update gauge dict with fetched info
                        gauge["name"] = gauge_name
                        if asset_info.get("description"):
                            gauge["description"] = asset_info["description"]
                except Exception:
                    pass  # Silently ignore, use fallback
            
            if not gauge_name:
                gauge_name = f"Asset {asset_id}"
                
            self.output._logger.info(f"  [{idx}/{total_gauges}] Processing: {gauge_name}")
            
            traces = traces_by_asset.get(asset_id, [])
            
            # Enrich each trace with alarms, thresholds, and timeseries
            enriched_traces = []
            for trace in traces:
                trace_id = safe_int(trace.get("id"))
                if trace_id is None:
                    continue
                
                # Add alarms
                enriched_trace = self.alarms.enrich_trace_with_alarms(
                    trace,
                    detailed_by_trace,
                    fetch_thresholds
                )
                
                # Add timeseries data
                # Determine data_type based on trace description
                # Rainfall traces require "Total", other traces use "Mean"
                trace_desc = trace.get("description", "").lower()
                if "rainfall" in trace_desc or "rain" in trace_desc:
                    data_type = "Total"
                else:
                    data_type = "Mean"
                
                try:
                    trace_data = self.traces.fetch_trace_data(
                        trace_id,
                        start_time,
                        end_time,
                        data_type=data_type
                    )
                    enriched_trace["data"] = trace_data
                except Exception as e:
                    self.output._logger.error(
                        f"Failed to fetch data for trace {trace_id}: {e}"
                    )
                    enriched_trace["data"] = {"items": [], "error": str(e)}
                
                enriched_traces.append(enriched_trace)
            
            # Combine gauge with enriched traces
            all_data.append({
                "gauge": gauge,
                "traces": enriched_traces
            })
        
        self.output._logger.info(f"✓ Enriched {len(all_data)} gauges")
        
        return all_data
    
    # Backwards compatibility methods
    def setup_temp_dir(self) -> None:
        """Setup temp directory (delegates to OutputManager)."""
        self.output.setup_temp_dir()
    
    def cleanup_temp_dir(self) -> None:
        """Cleanup temp directory (delegates to OutputManager)."""
        self.output.cleanup_temp_dir()
    
    def finalize_output(self) -> None:
        """Finalize output (delegates to OutputManager)."""
        self.output.finalize_output()
