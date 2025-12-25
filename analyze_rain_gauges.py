import logging

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.analyze.runner import run_filter_active_gauges
from moata_pipeline.common.constants import (INACTIVE_THRESHOLD_MONTHS, DEFAULT_EXCLUDE_KEYWORD)


def main() -> None:
    setup_logging("INFO")
    logging.info("=" * 80)
    logging.info("Starting rain gauge data filtering and analysis")
    logging.info("=" * 80)

    result = run_filter_active_gauges(inactive_months=INACTIVE_THRESHOLD_MONTHS, exclude_keyword=DEFAULT_EXCLUDE_KEYWORD)
    logging.info("\n%s", result["report"])

    logging.info("=" * 80)
    logging.info("COMPLETE!")
    logging.info("=" * 80)
    if "output_dir" in result:
        logging.info("Output files saved to: %s", result["output_dir"])
    else:
        logging.info("Output files saved (paths): %s", {k: v for k, v in result.items() if "path" in k or "file" in k or "csv" in k})

    logging.info("=" * 80)


if __name__ == "__main__":
    main()
