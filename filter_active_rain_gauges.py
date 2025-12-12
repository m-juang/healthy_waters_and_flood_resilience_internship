import logging

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.analyze.runner import run_filter_active_gauges


def main() -> None:
    setup_logging("INFO")
    logging.info("=" * 80)
    logging.info("Starting rain gauge data filtering and analysis (refactored)")
    logging.info("=" * 80)

    result = run_filter_active_gauges(inactive_months=3, exclude_keyword="northland")
    print("\n" + result["report"])

    logging.info("=" * 80)
    logging.info("COMPLETE!")
    logging.info("=" * 80)
    logging.info("Output files saved to: moata_filtered/")
    logging.info("=" * 80)


if __name__ == "__main__":
    main()
