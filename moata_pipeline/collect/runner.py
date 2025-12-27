"""
Collection Runner Module

Provides high-level entry point functions for running data collection tasks.
Used by entry point scripts (retrieve_rain_gauges.py, retrieve_rain_radar.py).

Functions:
    run_collect_rain_gauges: Collect rain gauge data with traces and alarms
    run_collect_radar: Collect radar QPE data for stormwater catchments

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List

from dotenv import load_dotenv

from moata_pipeline.common.constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    DEFAULT_PROJECT_ID,
    DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID,
    TOKEN_TTL_SECONDS,
    TOKEN_REFRESH_BUFFER_SECONDS,
    DEFAULT_REQUESTS_PER_SECOND,
)
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.common.output_writer import JsonOutputWriter
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient
from moata_pipeline.collect.collector import RainGaugeCollector, RadarDataCollector


# Version info
__version__ = "1.0.0"

# Default time window for recent vs historical classification
RECENT_DATA_THRESHOLD_HOURS = 24


# =============================================================================
# Custom Exceptions
# =============================================================================

class CollectionRunnerError(Exception):
    """Base exception for collection runner errors."""
    pass


class CredentialsError(CollectionRunnerError):
    """Raised when API credentials are missing or invalid."""
    pass


class ClientCreationError(CollectionRunnerError):
    """Raised when MoataClient creation fails."""
    pass


# =============================================================================
# Helper Functions
# =============================================================================

def _create_client() -> MoataClient:
    """
    Create authenticated MoataClient with credentials from .env file.
    
    Returns:
        Authenticated MoataClient instance
        
    Raises:
        CredentialsError: If credentials are missing
        ClientCreationError: If client creation fails
        
    Example:
        >>> client = _create_client()
        >>> gauges = client.get_rain_gauges(project_id=594, asset_type_id=100)
    """
    logger = logging.getLogger(__name__)
    
    # Load environment variables
    try:
        load_dotenv(dotenv_path=Path.cwd() / ".env")
    except Exception as e:
        logger.warning(f"Failed to load .env file: {e}")
    
    # Get credentials
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise CredentialsError(
            "MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set in .env file.\n\n"
            "How to fix:\n"
            "1. Create .env file in project root\n"
            "2. Add credentials:\n"
            "   MOATA_CLIENT_ID=your_client_id\n"
            "   MOATA_CLIENT_SECRET=your_client_secret\n"
            "3. See .env.example for template"
        )
    
    logger.debug("Creating authenticated MoataClient...")
    logger.debug(f"  Token URL: {TOKEN_URL}")
    logger.debug(f"  Base API URL: {BASE_API_URL}")
    
    try:
        # Create auth handler
        auth = MoataAuth(
            token_url=TOKEN_URL,
            scope=OAUTH_SCOPE,
            client_id=client_id,
            client_secret=client_secret,
            verify_ssl=False,  # Disabled for Auckland Council network compatibility
            ttl_seconds=TOKEN_TTL_SECONDS,
            refresh_buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS,
        )
        
        # Create HTTP client
        http = MoataHttp(
            get_token_fn=auth.get_token,
            base_url=BASE_API_URL,
            requests_per_second=DEFAULT_REQUESTS_PER_SECOND,
            verify_ssl=False,  # Disabled for Auckland Council network compatibility
        )
        
        # Create API client
        client = MoataClient(http=http)
        
        logger.debug("✓ MoataClient created successfully")
        return client
        
    except Exception as e:
        logger.error(f"Failed to create MoataClient: {e}")
        raise ClientCreationError(
            f"Failed to create MoataClient: {e}\n\n"
            f"Possible causes:\n"
            f"1. Invalid credentials in .env\n"
            f"2. Network connectivity issues\n"
            f"3. Moata API is unavailable\n"
            f"4. SSL certificate issues"
        ) from e


def _determine_radar_output_dir(
    start_time: datetime,
    end_time: datetime,
    custom_dir: Optional[Path] = None,
) -> Path:
    """
    Determine output directory for radar data based on time range.
    
    Logic:
        - If custom_dir provided: use it
        - If data is recent (within 24h of now): outputs/rain_radar/raw
        - If data is historical: outputs/rain_radar/historical/YYYY-MM-DD/raw
        
    Args:
        start_time: Data start time (UTC)
        end_time: Data end time (UTC)
        custom_dir: Optional custom directory
        
    Returns:
        Path to output directory
    """
    logger = logging.getLogger(__name__)
    
    if custom_dir is not None:
        logger.info(f"Using custom output directory: {custom_dir}")
        return custom_dir
    
    # Check if data is recent (within last 24 hours)
    now = datetime.now(timezone.utc)
    hours_since_end = (now - end_time).total_seconds() / 3600
    
    is_recent = hours_since_end < RECENT_DATA_THRESHOLD_HOURS
    
    if is_recent:
        output_dir = Path("outputs/rain_radar/raw")
        logger.info("Recent data (within 24h) - output to: %s", output_dir)
    else:
        # Historical data - use date-based directory
        date_str = start_time.strftime("%Y-%m-%d")
        output_dir = Path(f"outputs/rain_radar/historical/{date_str}/raw")
        logger.info("Historical data (%s) - output to: %s", date_str, output_dir)
    
    return output_dir


# =============================================================================
# Public Runner Functions
# =============================================================================

def run_collect_rain_gauges(
    project_id: int = DEFAULT_PROJECT_ID,
    asset_type_id: int = DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID,
    trace_batch_size: int = 100,
    fetch_thresholds: bool = True,
) -> None:
    """
    Collect rain gauge data with traces, alarms, and thresholds.
    
    High-level function that:
        1. Creates authenticated API client
        2. Collects all rain gauge assets
        3. Fetches traces for each gauge
        4. Fetches alarms and thresholds for each trace
        5. Saves combined JSON output
        
    Args:
        project_id: Moata project ID (default from constants)
        asset_type_id: Rain gauge asset type ID (default from constants)
        trace_batch_size: Traces to fetch per API batch (default: 100)
        fetch_thresholds: Whether to fetch alarm thresholds (default: True)
        
    Raises:
        CredentialsError: If API credentials missing
        ClientCreationError: If client creation fails
        CollectionRunnerError: If collection fails
        
    Example:
        >>> run_collect_rain_gauges(
        ...     project_id=594,
        ...     asset_type_id=100,
        ...     trace_batch_size=50
        ... )
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("Rain Gauge Data Collection")
    logger.info("=" * 80)
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Asset Type ID: {asset_type_id}")
    logger.info(f"Trace Batch Size: {trace_batch_size}")
    logger.info(f"Fetch Thresholds: {fetch_thresholds}")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        # Create authenticated client
        logger.info("Initializing API client...")
        client = _create_client()
        logger.info("✓ API client ready")
        
        # Initialize paths
        paths = PipelinePaths()
        logger.info(f"Output directory: {paths.rain_gauges_raw_dir}")
        
        # Create collector and collect data
        logger.info("")
        logger.info("Starting collection...")
        collector = RainGaugeCollector(client=client)
        
        all_data = collector.collect(
            project_id=project_id,
            asset_type_id=asset_type_id,
            trace_batch_size=trace_batch_size,
            fetch_thresholds=fetch_thresholds,
        )
        
        # Save output
        logger.info("")
        logger.info("Saving data...")
        writer = JsonOutputWriter(out_dir=paths.rain_gauges_raw_dir)
        output_path = writer.write_combined(all_data)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Collection Complete")
        logger.info("=" * 80)
        logger.info(f"Gauges collected: {len(all_data)}")
        logger.info(f"Output file: {output_path}")
        logger.info("=" * 80)
        
    except CredentialsError as e:
        logger.error("Credentials error:")
        logger.error(str(e))
        raise
        
    except ClientCreationError as e:
        logger.error("Client creation error:")
        logger.error(str(e))
        raise
        
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        logger.exception("Full traceback:")
        raise CollectionRunnerError(
            f"Rain gauge collection failed: {e}\n\n"
            f"Check logs above for details."
        ) from e


def run_collect_radar(
    project_id: int = DEFAULT_PROJECT_ID,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    catchment_ids: Optional[List[int]] = None,
    force_refresh_pixels: bool = False,
    output_dir: Optional[Path] = None,
    pixel_batch_size: int = 50,
) -> None:
    """
    Collect radar QPE data for stormwater catchments.
    
    High-level function that:
        1. Creates authenticated API client
        2. Fetches stormwater catchments
        3. Gets pixel mappings for each catchment
        4. Fetches radar timeseries data
        5. Saves individual catchment CSVs
        6. Saves collection summary JSON
        
    Args:
        project_id: Moata project ID (default from constants)
        start_time: Start of time range (default: 24 hours ago)
        end_time: End of time range (default: now)
        catchment_ids: Optional list of specific catchment IDs to collect
        force_refresh_pixels: If True, rebuild pixel mappings from API
        output_dir: Custom output directory (default: auto based on date)
        pixel_batch_size: Pixels per API request (default: 50)
        
    Raises:
        CredentialsError: If API credentials missing
        ClientCreationError: If client creation fails
        CollectionRunnerError: If collection fails
        
    Example:
        >>> # Collect last 24 hours
        >>> run_collect_radar()
        
        >>> # Collect specific date
        >>> run_collect_radar(
        ...     start_time=datetime(2025, 5, 9, tzinfo=timezone.utc),
        ...     end_time=datetime(2025, 5, 10, tzinfo=timezone.utc)
        ... )
        
        >>> # Collect specific catchments only
        >>> run_collect_radar(catchment_ids=[123, 456, 789])
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("Radar QPE Data Collection")
    logger.info("=" * 80)
    logger.info(f"Project ID: {project_id}")
    
    # Determine time range
    if end_time is None:
        end_time = datetime.now(timezone.utc)
    if start_time is None:
        start_time = end_time - timedelta(hours=24)
    
    logger.info(f"Time Range:")
    logger.info(f"  Start: {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"  End:   {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"  Duration: {(end_time - start_time).total_seconds() / 3600:.1f} hours")
    
    if catchment_ids:
        logger.info(f"Specific catchments: {len(catchment_ids)} IDs")
    else:
        logger.info("Catchments: All available")
    
    logger.info(f"Force refresh pixels: {force_refresh_pixels}")
    logger.info(f"Pixel batch size: {pixel_batch_size}")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        # Create authenticated client
        logger.info("Initializing API client...")
        client = _create_client()
        logger.info("✓ API client ready")
        
        # Determine output directory
        output_path = _determine_radar_output_dir(start_time, end_time, output_dir)
        
        # Create collector
        logger.info("")
        logger.info("Initializing radar collector...")
        collector = RadarDataCollector(
            client=client,
            output_dir=output_path,
            pixel_batch_size=pixel_batch_size,
            max_hours_per_request=24,
        )
        logger.info("✓ Collector ready")
        
        # Collect data
        logger.info("")
        logger.info("Starting collection...")
        results = collector.collect_all(
            project_id=project_id,
            start_time=start_time,
            end_time=end_time,
            catchment_ids=catchment_ids,
            force_refresh_pixels=force_refresh_pixels,
        )
        
        # Generate summary statistics
        total_catchments = len(results)
        successful = len([r for r in results if not r.get("error")])
        failed = len([r for r in results if r.get("error")])
        total_pixels = sum(r.get("pixel_count", 0) for r in results)
        total_data_records = sum(r.get("data_records", 0) for r in results)
        
        # Log summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Collection Complete")
        logger.info("=" * 80)
        logger.info(f"Catchments processed: {total_catchments}")
        logger.info(f"  Successful: {successful}")
        logger.info(f"  Failed: {failed}")
        logger.info(f"Total pixels: {total_pixels}")
        logger.info(f"Total data records: {total_data_records}")
        logger.info(f"Output directory: {output_path}")
        logger.info("=" * 80)
        
        # Log errors if any
        if failed > 0:
            logger.warning("")
            logger.warning("Failed catchments:")
            for r in results:
                if r.get("error"):
                    logger.warning(f"  - {r.get('catchment_name')}: {r.get('error')}")
        
    except CredentialsError as e:
        logger.error("Credentials error:")
        logger.error(str(e))
        raise
        
    except ClientCreationError as e:
        logger.error("Client creation error:")
        logger.error(str(e))
        raise
        
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        logger.exception("Full traceback:")
        raise CollectionRunnerError(
            f"Radar collection failed: {e}\n\n"
            f"Check logs above for details."
        ) from e