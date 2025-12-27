#!/usr/bin/env python3
"""
Rain Gauge Data Collection Script

Collects rain gauge data from the Moata API for Auckland Council's rain monitoring network.
Fetches data for all active rain gauges and saves raw responses to outputs/rain_gauges/raw/.

Usage:
    python retrieve_rain_gauges.py [options]

Examples:
    # Collect data with default settings (INFO logging)
    python retrieve_rain_gauges.py
    
    # Enable debug logging
    python retrieve_rain_gauges.py --log-level DEBUG
    
    # Quiet mode (errors only)
    python retrieve_rain_gauges.py --log-level ERROR

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import argparse
import logging
import sys
from pathlib import Path

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.collect.runner import run_collect_rain_gauges


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Collect rain gauge data from Moata API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Collect with default settings
  %(prog)s --log-level DEBUG        # Enable debug logging
  %(prog)s --log-level ERROR        # Quiet mode (errors only)

Output:
  Raw data saved to: outputs/rain_gauges/raw/
  Collection summary: outputs/rain_gauges/raw/collection_summary.json

Duration:
  Typically 5-10 minutes depending on API response times.
        """
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0"
    )
    
    return parser.parse_args()


def main() -> int:
    """
    Main entry point for rain gauge data collection.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()
    
    # Setup logging with user-specified level
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Rain Gauge Data Collection")
        logger.info("=" * 60)
        
        # Run collection
        run_collect_rain_gauges()
        
        logger.info("=" * 60)
        logger.info("Rain gauge data collection completed successfully")
        logger.info("=" * 60)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Collection interrupted by user (Ctrl+C)")
        logger.info("Partial data may have been saved to outputs/rain_gauges/raw/")
        return 130  # Standard exit code for SIGINT
        
    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        logger.error("Ensure the project structure is intact and outputs/ directory exists")
        return 1
        
    except PermissionError as e:
        logger.error(f"❌ Permission denied: {e}")
        logger.error("Check file/directory permissions for outputs/")
        return 1
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ COLLECTION FAILED")
        logger.error("=" * 60)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.exception("Full traceback:")
        logger.error("")
        logger.error("Troubleshooting tips:")
        logger.error("1. Check your .env file has correct MOATA_CLIENT_ID and MOATA_CLIENT_SECRET")
        logger.error("2. Verify network connection to Moata API")
        logger.error("3. Check outputs/rain_gauges/ directory permissions")
        logger.error("4. Review logs above for specific error details")
        logger.error("5. Try running with --log-level DEBUG for more information")
        return 1


if __name__ == "__main__":
    sys.exit(main())