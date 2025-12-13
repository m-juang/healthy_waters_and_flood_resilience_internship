from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

# ✅ FIXED: Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from moata_pipeline.moata.client import MoataClient


class TimeSeriesFetcher:
    """Fetch time series data from Moata API for alarm validation."""
    
    def __init__(self, client: MoataClient) -> None:
        self._client = client
    
    def fetch_trace_data(
        self,
        trace_id: int,
        from_time: datetime,
        to_time: datetime,
        data_type: str = "None",
        data_interval: int = 300  # 5 minutes
    ) -> pd.DataFrame:
        """
        Fetch raw time series data for a trace.
        
        Args:
            trace_id: Moata trace ID
            from_time: Start datetime (ISO format)
            to_time: End datetime (ISO format)
            data_type: 'None' for raw data
            data_interval: Seconds between points (300 = 5 min)
        
        Returns:
            DataFrame with columns: timestamp, value
        
        Raises:
            ValueError: If time range exceeds 32-day limit for virtual traces
        """
        # Validate 32-day limit for virtual traces (ARI traces)
        if (to_time - from_time).days > 32:
            raise ValueError(
                f"Time range exceeds 32-day limit for virtual traces. "
                f"Requested: {(to_time - from_time).days} days"
            )
        
        # Prepare API parameters
        params = {
            "from": from_time.isoformat(),
            "to": to_time.isoformat(),
            "dataType": data_type,
            "dataInterval": data_interval
        }
        
        # Call API endpoint: GET /traces/{traceId}/data
        data = self._client.get_trace_data(trace_id, params)
        
        # Convert to DataFrame for analysis
        return self._parse_timeseries(data)
    
    def _parse_timeseries(self, data: Any) -> pd.DataFrame:
        """
        Convert API response to pandas DataFrame.
        
        Args:
            data: API response (expected format: list of {time, value} dicts 
                  or wrapped in {data: [...]} structure)
        
        Returns:
            DataFrame with columns: timestamp (datetime), value (float)
        """
        # Handle wrapped response
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        
        # Handle empty response
        if not data or not isinstance(data, list):
            return pd.DataFrame(columns=["timestamp", "value"])
        
        # Parse data points
        records = []
        for point in data:
            # Expected format: {"time": "2024-11-15T05:00:00Z", "value": 6.8}
            # Or: {"timestamp": ..., "value": ...}
            timestamp = point.get("time") or point.get("timestamp")
            value = point.get("value")
            
            if timestamp and value is not None:
                records.append({
                    "timestamp": pd.to_datetime(timestamp),
                    "value": float(value)
                })
        
        df = pd.DataFrame(records)
        
        # Sort by timestamp
        if not df.empty:
            df = df.sort_values("timestamp").reset_index(drop=True)
        
        return df
    
    def fetch_trace_data_chunked(
        self,
        trace_id: int,
        from_time: datetime,
        to_time: datetime,
        data_type: str = "None",
        data_interval: int = 300,
        chunk_days: int = 30
    ) -> pd.DataFrame:
        """
        Fetch time series data in chunks to avoid 32-day limit.
        
        Useful for fetching data over longer periods by breaking into 
        multiple API calls.
        
        Args:
            trace_id: Moata trace ID
            from_time: Start datetime
            to_time: End datetime
            data_type: 'None' for raw data
            data_interval: Seconds between points
            chunk_days: Days per chunk (default 30, must be <= 32)
        
        Returns:
            Combined DataFrame with all data
        """
        if chunk_days > 32:
            raise ValueError("chunk_days must be <= 32")
        
        all_data = []
        current_start = from_time
        
        while current_start < to_time:
            current_end = min(current_start + timedelta(days=chunk_days), to_time)
            
            chunk_df = self.fetch_trace_data(
                trace_id=trace_id,
                from_time=current_start,
                to_time=current_end,
                data_type=data_type,
                data_interval=data_interval
            )
            
            if not chunk_df.empty:
                all_data.append(chunk_df)
            
            current_start = current_end
        
        # Combine all chunks
        if not all_data:
            return pd.DataFrame(columns=["timestamp", "value"])
        
        combined = pd.concat(all_data, ignore_index=True)
        combined = combined.sort_values("timestamp").reset_index(drop=True)
        
        # Remove duplicates at chunk boundaries
        combined = combined.drop_duplicates(subset=["timestamp"], keep="first")
        
        return combined