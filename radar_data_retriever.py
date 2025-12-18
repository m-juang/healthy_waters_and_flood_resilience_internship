"""
Main script to run radar data collection
Integrates with existing moata_pipeline
"""
import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=Path.cwd() / ".env")

from moata_pipeline.common.constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    DEFAULT_PROJECT_ID,
    TOKEN_TTL_SECONDS,
    TOKEN_REFRESH_BUFFER_SECONDS,
    DEFAULT_REQUESTS_PER_SECOND,
)
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.radar_client import RadarClient
from moata_pipeline.collect.radar_collector import RadarDataCollector
from moata_pipeline.logging_setup import setup_logging

# ============================================================================
# CONFIGURATION
# ============================================================================

# Output directory
OUTPUT_DIR = Path("radar_data_output")

# ============================================================================
# MAIN SCRIPT
# ============================================================================

def main():
    """Run radar data collection"""
    
    # Setup logging
    setup_logging(level="INFO")
    logger = logging.getLogger(__name__)
    
    logger.info("="*80)
    logger.info("RAIN RADAR DATA COLLECTOR")
    logger.info("="*80)
    
    # Get credentials from environment
    client_id = os.getenv("MOATA_CLIENT_ID")
    client_secret = os.getenv("MOATA_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        logger.error("="*80)
        logger.error("CONFIGURATION ERROR")
        logger.error("="*80)
        logger.error("MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set in .env file")
        logger.error("="*80)
        return
    
    # Initialize authentication
    logger.info("\nInitializing authentication...")
    auth = MoataAuth(
        token_url=TOKEN_URL,
        scope=OAUTH_SCOPE,
        client_id=client_id,
        client_secret=client_secret,
        verify_ssl=False,
        ttl_seconds=TOKEN_TTL_SECONDS,
        refresh_buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS,
    )
    
    # Initialize HTTP client
    http = MoataHttp(
        get_token_fn=auth.get_token,
        base_url=BASE_API_URL,
        requests_per_second=DEFAULT_REQUESTS_PER_SECOND,
        verify_ssl=False,
        timeout_seconds=60,
        connect_timeout_seconds=15,
        max_retries=5,
        backoff_factor=1.0
    )
    
    # Initialize radar client
    radar_client = RadarClient(http)
    
    # Initialize collector
    collector = RadarDataCollector(
        radar_client=radar_client,
        project_id=DEFAULT_PROJECT_ID,  # 594 for Auckland Council
        output_dir=OUTPUT_DIR,
        pixel_batch_size=50,  # Sam recommends 50 pixels per batch
        max_hours_per_request=24  # Sam recommends max 24 hours
    )
    
    # Define time range
    # Example: Last 24 hours
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=24)
    
    # Or specific date range:
    # start_time = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    # end_time = datetime(2024, 1, 16, 0, 0, 0, tzinfo=timezone.utc)
    
    logger.info(f"\nTime range: {start_time} to {end_time}")
    
    # Run collection
    # To download specific catchments only, pass catchment_ids=[123, 456]
    # To refresh pixel mappings, pass force_refresh_pixels=True
    results = collector.download_all_catchments(
        start_time=start_time,
        end_time=end_time,
        catchment_ids=None,  # None = all catchments
        force_refresh_pixels=False  # False = use cached mappings
    )
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("COLLECTION SUMMARY")
    logger.info("="*80)
    logger.info(f"Successfully downloaded data for {len(results)} catchments")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("="*80)
    
    logger.info("\nNext steps:")
    logger.info("1. Wait for Sam's ARI lookup table")
    logger.info("2. Run ARI analysis on downloaded data")
    logger.info("3. Calculate area exceedance proportions")


if __name__ == "__main__":
    main()
