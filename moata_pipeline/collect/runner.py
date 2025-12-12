from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path

# Load from project root (same folder as moata_data_retriever.py)
load_dotenv(dotenv_path=Path.cwd() / ".env")


from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient
from moata_pipeline.collect.collector import RainGaugeCollector


def run_collect_rain_gauges(
    project_id: int = 594,
    asset_type_id: int = 100,
    out_dir: Path = Path("moata_output"),
) -> None:
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set")

    TOKEN_URL = (
        "https://moata.b2clogin.com/"
        "moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token"
    )
    BASE_API_URL = "https://api.moata.io/ae/v1"
    SCOPE = "https://moata.onmicrosoft.com/moata.io/.default"

    auth = MoataAuth(
        token_url=TOKEN_URL,
        scope=SCOPE,
        client_id=client_id,
        client_secret=client_secret,
        verify_ssl=False,
        ttl_seconds=3600,
        refresh_buffer_seconds=300,
    )

    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        requests_per_second=2.0,
        verify_ssl=False,
    )

    client = MoataClient(http=http)
    collector = RainGaugeCollector(client=client, out_dir=out_dir)

    logging.info("Starting collection: project=%s assetTypeId=%s", project_id, asset_type_id)
    collector.collect(project_id=project_id, asset_type_id=asset_type_id)
