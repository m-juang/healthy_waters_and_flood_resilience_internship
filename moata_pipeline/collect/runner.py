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

logger = logging.getLogger(__name__)


def _create_client() -> MoataClient:
    """Create authenticated MoataClient."""
    load_dotenv(dotenv_path=Path.cwd() / ".env")

    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set")

    auth = MoataAuth(
        token_url=TOKEN_URL,
        scope=OAUTH_SCOPE,
        client_id=client_id,
        client_secret=client_secret,
        verify_ssl=False,
        ttl_seconds=TOKEN_TTL_SECONDS,
        refresh_buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS,
    )

    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        requests_per_second=DEFAULT_REQUESTS_PER_SECOND,
        verify_ssl=False,
    )

    return MoataClient(http=http)


def run_collect_rain_gauges(
    project_id: int = DEFAULT_PROJECT_ID,
    asset_type_id: int = DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID,
) -> None:
    """Collect rain gauge data with traces and alarms."""
    logger.info("=" * 80)
    logger.info("RAIN GAUGE DATA COLLECTOR")
    logger.info("=" * 80)

    client = _create_client()
    paths = PipelinePaths()

    collector = RainGaugeCollector(client=client)
    all_data = collector.collect(project_id=project_id, asset_type_id=asset_type_id)

    writer = JsonOutputWriter(out_dir=paths.rain_gauges_raw_dir)
    writer.write_combined(all_data)

    logger.info("✓ Saved combined structure: %s", paths.rain_gauges_traces_alarms_json)


def run_collect_radar(
    project_id: int = DEFAULT_PROJECT_ID,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    catchment_ids: Optional[List[int]] = None,
    force_refresh_pixels: bool = False,
    output_dir: Optional[Path] = None,
) -> None:
    """
    Collect radar QPE data for stormwater catchments.
    
    Args:
        project_id: Project ID (default 594)
        start_time: Start of time range (default: 24 hours ago)
        end_time: End of time range (default: now)
        catchment_ids: Optional list of specific catchment IDs
        force_refresh_pixels: If True, rebuild pixel mappings from API
        output_dir: Custom output directory (default: auto based on date)
    """
    logger.info("=" * 80)
    logger.info("RADAR DATA COLLECTOR")
    logger.info("=" * 80)

    client = _create_client()

    # Default time range: last 24 hours
    if end_time is None:
        end_time = datetime.now(timezone.utc)
    if start_time is None:
        start_time = end_time - timedelta(hours=24)

    logger.info("Time range: %s to %s", start_time, end_time)

    # Determine output directory
    if output_dir is None:
        # Check if this is historical data (not "last 24 hours")
        now = datetime.now(timezone.utc)
        is_recent = (now - end_time).total_seconds() < 86400  # within 24 hours of now
        
        if is_recent:
            output_dir = Path("outputs/rain_radar/raw")
        else:
            # Historical data - use date-based directory
            date_str = start_time.strftime("%Y-%m-%d")
            output_dir = Path(f"outputs/rain_radar/historical/{date_str}/raw")
            logger.info("Historical data - output to: %s", output_dir)

    collector = RadarDataCollector(
        client=client,
        output_dir=output_dir,
        pixel_batch_size=50,
        max_hours_per_request=24,
    )

    results = collector.collect_all(
        project_id=project_id,
        start_time=start_time,
        end_time=end_time,
        catchment_ids=catchment_ids,
        force_refresh_pixels=force_refresh_pixels,
    )

    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("COLLECTION SUMMARY")
    logger.info("=" * 80)

    total_pixels = sum(r.get("pixel_count", 0) for r in results)
    total_data_records = sum(r.get("data_records", 0) for r in results)
    errors = [r for r in results if r.get("error")]

    logger.info("Catchments processed: %d", len(results))
    logger.info("Total pixels: %d", total_pixels)
    logger.info("Total data records: %d", total_data_records)
    logger.info("Errors: %d", len(errors))
    logger.info("Output directory: %s", output_dir)
    logger.info("=" * 80)
