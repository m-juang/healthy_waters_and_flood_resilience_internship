from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# ✅ FIXED: Removed duplicate "from pathlib import Path"

# Load from project root (same folder as moata_data_retriever.py)
load_dotenv(dotenv_path=Path.cwd() / ".env")

# ✅ FIXED: Import konstanta dari constants.py (tidak duplikasi lagi!)
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

from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient
from moata_pipeline.collect.collector import RainGaugeCollector


def run_collect_rain_gauges(
    project_id: int = DEFAULT_PROJECT_ID,
    asset_type_id: int = DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID,
    out_dir: Path = Path("moata_output"),
) -> None:
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set")

    # ✅ FIXED: Gunakan konstanta dari constants.py
    auth = MoataAuth(
        token_url=TOKEN_URL,
        scope=OAUTH_SCOPE,  # ✅ nama yang benar: OAUTH_SCOPE (bukan SCOPE)
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

    client = MoataClient(http=http)
    from moata_pipeline.common.output_writer import JsonOutputWriter

    collector = RainGaugeCollector(client=client)
    writer = JsonOutputWriter(out_dir=out_dir)

    all_data = collector.collect(project_id=project_id, asset_type_id=asset_type_id)

    writer.write_combined(all_data)
    logging.info("✓ Saved combined structure: %s", out_dir / "rain_gauges_traces_alarms.json")
