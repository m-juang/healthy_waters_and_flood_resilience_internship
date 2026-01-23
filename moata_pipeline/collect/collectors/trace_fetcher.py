"""Trace fetching logic."""
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base import BaseCollector, HTTPClientProtocol
from moata_pipeline.common.typing_utils import safe_int
from moata_pipeline.common.time_utils import iso_z
from moata_pipeline.common.iter_utils import chunk


class TraceFetcher(BaseCollector):
    """
    Fetches trace and timeseries data.
    
    Single Responsibility: Trace operations only
    
    Responsibilities:
    - Fetch traces for assets (batched)
    - Fetch timeseries data for traces
    - Handle batching logic
    
    Example:
        >>> fetcher = TraceFetcher(client)
        >>> traces_by_asset = fetcher.fetch_traces_batched(asset_ids, batch_size=100)
        >>> trace_data = fetcher.fetch_trace_data(12345, start_time, end_time)
    """
    
    def fetch_traces_batched(
        self,
        asset_ids: List[int],
        batch_size: int = 100,
        asset_names: Optional[Dict[int, Dict[str, Any]]] = None
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Fetch traces for multiple assets in batches.
        
        Args:
            asset_ids: List of asset IDs
            batch_size: Number of assets per batch
            asset_names: Optional dict mapping asset_id -> gauge dict (for logging)
            
        Returns:
            Dictionary mapping asset_id -> list of traces
            
        Raises:
            ValueError: If batch_size is invalid
            
        Example:
            >>> asset_ids = [1, 2, 3, 4, 5]
            >>> traces = fetcher.fetch_traces_batched(asset_ids, batch_size=2)
            >>> print(f"Asset 1 has {len(traces[1])} traces")
        """
        self._validate_positive_int(batch_size, "batch_size")
        
        if not asset_ids:
            self._logger.warning("No asset IDs provided for trace fetching")
            return {}
        
        self._logger.info(f"Fetching traces for {len(asset_ids)} assets...")
        self._logger.info(f"  Batch size: {batch_size}")
        
        traces_by_asset: Dict[int, List[Dict[str, Any]]] = {}
        batches = list(chunk(asset_ids, batch_size))
        
        self._logger.info(f"  Processing {len(batches)} batches...")
        
        processed_count = 0
        total_assets = len(asset_ids)
        
        for batch_idx, batch_ids in enumerate(batches, 1):
            self._logger.debug(f"  Batch {batch_idx}/{len(batches)}: {len(batch_ids)} assets")
            
            for asset_id in batch_ids:
                processed_count += 1
                # Get gauge name for better logging
                gauge_name = ""
                if asset_names and asset_id in asset_names:
                    gauge_name = asset_names[asset_id].get("name", "")
                
                # If no name available and client supports get_asset_by_id, try to fetch it
                if not gauge_name and hasattr(self._client, 'get_asset_by_id'):
                    try:
                        asset_info = self._client.get_asset_by_id(asset_id)
                        if asset_info:
                            gauge_name = asset_info.get("name", "")
                    except Exception:
                        pass  # Silently ignore, we'll just log asset ID
                
                try:
                    traces = self._client.get_traces_for_asset(asset_id)
                    traces_by_asset[asset_id] = traces
                    
                    # Log with gauge name if available (with progress)
                    if gauge_name:
                        self._logger.info(f"  [{processed_count}/{total_assets}] [{len(traces)} traces] {gauge_name}")
                    elif traces:
                        self._logger.info(f"  [{processed_count}/{total_assets}] Asset {asset_id}: {len(traces)} traces")
                    else:
                        self._logger.debug(f"  [{processed_count}/{total_assets}] Asset {asset_id}: no traces")
                        
                except Exception as e:
                    if gauge_name:
                        self._logger.error(f"  ✗ Failed: {gauge_name} - {e}")
                    else:
                        self._logger.error(f"    Failed to fetch traces for asset {asset_id}: {e}")
                    traces_by_asset[asset_id] = []
        
        total_traces = sum(len(traces) for traces in traces_by_asset.values())
        self._logger.info(f"✓ Fetched {total_traces} traces across {len(traces_by_asset)} assets")
        
        return traces_by_asset
    
    def fetch_trace_data(
        self,
        trace_id: int,
        start_time: datetime,
        end_time: datetime,
        data_type: str = "Mean"
    ) -> Dict[str, Any]:
        """
        Fetch timeseries data for a trace.
        
        Args:
            trace_id: Trace ID
            start_time: Start of time range
            end_time: End of time range
            data_type: Data aggregation type (default: "Mean")
            
        Returns:
            Timeseries data dictionary
            
        Raises:
            ValueError: If parameters are invalid
            
        Example:
            >>> from datetime import datetime, timezone
            >>> start = datetime(2025, 1, 1, tzinfo=timezone.utc)
            >>> end = datetime(2025, 1, 2, tzinfo=timezone.utc)
            >>> data = fetcher.fetch_trace_data(12345, start, end)
        """
        self._validate_positive_int(trace_id, "trace_id")
        self._validate_time_range(start_time, end_time)
        
        from_time = iso_z(start_time)
        to_time = iso_z(end_time)
        
        self._logger.debug(f"Fetching trace data for trace {trace_id}")
        self._logger.debug(f"  From: {from_time}")
        self._logger.debug(f"  To: {to_time}")
        self._logger.debug(f"  Data type: {data_type}")
        
        try:
            data = self._client.get_trace_data(
                trace_id=trace_id,
                from_time=from_time,
                to_time=to_time,
                data_type=data_type,
                data_interval=300  # 5-minute resolution per Sam's guidance
            )
            
            # Log data summary
            if isinstance(data, dict) and "items" in data:
                item_count = len(data.get("items", []))
                self._logger.debug(f"  ✓ Got {item_count} data points")
            
            return data
            
        except Exception as e:
            self._logger.error(f"Failed to fetch trace data for {trace_id}: {e}")
            raise
    
    def fetch_trace_data_batch(
        self,
        trace_ids: List[int],
        start_time: datetime,
        end_time: datetime,
        data_type: str = "Mean"
    ) -> Dict[int, Dict[str, Any]]:
        """
        Fetch trace data for multiple traces.
        
        Args:
            trace_ids: List of trace IDs
            start_time: Start of time range
            end_time: End of time range
            data_type: Data aggregation type
            
        Returns:
            Dictionary mapping trace_id -> trace data
            
        Example:
            >>> trace_ids = [1, 2, 3]
            >>> data_by_trace = fetcher.fetch_trace_data_batch(trace_ids, start, end)
        """
        if not trace_ids:
            self._logger.warning("No trace IDs provided")
            return {}
        
        self._logger.info(f"Fetching data for {len(trace_ids)} traces...")
        
        data_by_trace: Dict[int, Dict[str, Any]] = {}
        
        for idx, trace_id in enumerate(trace_ids, 1):
            try:
                data = self.fetch_trace_data(trace_id, start_time, end_time, data_type)
                data_by_trace[trace_id] = data
                
                if idx % 10 == 0:
                    self._logger.info(f"  Progress: {idx}/{len(trace_ids)} traces")
                    
            except Exception as e:
                self._logger.error(f"Failed to fetch data for trace {trace_id}: {e}")
                data_by_trace[trace_id] = {"items": [], "error": str(e)}
        
        self._logger.info(f"✓ Fetched data for {len(data_by_trace)} traces")
        
        return data_by_trace