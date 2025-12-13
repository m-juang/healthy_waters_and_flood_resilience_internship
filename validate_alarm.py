from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.common.constants import TOKEN_URL, BASE_API_URL, OAUTH_SCOPE
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient
from moata_pipeline.validate.runner import run_alarm_validation

load_dotenv()
setup_logging("INFO")


def main():
    # Setup API client (same as moata_data_retriever.py)
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise RuntimeError("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set")
    
    auth = MoataAuth(
        token_url=TOKEN_URL,
        scope=OAUTH_SCOPE,
        client_id=client_id,
        client_secret=client_secret,
        verify_ssl=False
    )
    
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        requests_per_second=2.0,
        verify_ssl=False
    )
    
    client = MoataClient(http=http)
    
    # Run validation
    alarm_log = Path("data/alarm_log_ari.csv")  # Sam's provided file
    output_dir = Path("data/validated_alarms")
    
    logging.info("=" * 80)
    logging.info("Starting ARI alarm validation")
    logging.info("=" * 80)
    
    results = run_alarm_validation(
        alarm_log_csv=alarm_log,
        output_dir=output_dir,
        client=client,
        sample_size=5  # Start with 5 gauges
    )
    
    logging.info("=" * 80)
    logging.info("Validation complete!")
    logging.info(f"Processed {len(results)} gauges")
    logging.info(f"Results saved to: {output_dir}")
    logging.info("=" * 80)


if __name__ == "__main__":
    main()