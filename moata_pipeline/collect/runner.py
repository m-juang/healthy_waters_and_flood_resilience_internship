from __future__ import annotations

import os
import logging
from pathlib import Path

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
from moata_pipeline.collect.collector import RainGaugeCollector


# Configure logging once at entrypoint
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def run_collect_rain_gauges(
    project_id: int = DEFAULT_PROJECT_ID,
    asset_type_id: int = DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID,
) -> None:
    # Load .env from project root
    load_dotenv(dotenv_path=Path.cwd() / ".env")

    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set")

    # Canonical output paths
    paths = PipelinePaths()

    # Auth + HTTP
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

    # Client + Collector
    client = MoataClient(http=http)
    collector = RainGaugeCollector(client=client)

    # Collect
    all_data = collector.collect(project_id=project_id, asset_type_id=asset_type_id)

    # Write to outputs/rain_gauges/raw
    writer = JsonOutputWriter(out_dir=paths.rain_gauges_raw_dir)
    writer.write_combined(all_data)

    logging.info("✓ Saved combined structure: %s", paths.rain_gauges_traces_alarms_json)


if __name__ == "__main__":
    run_collect_rain_gauges()
