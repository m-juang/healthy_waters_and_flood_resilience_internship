"""Entry point for radar data collection."""
from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.collect.runner import run_collect_radar

setup_logging("INFO")

if __name__ == "__main__":
    run_collect_radar()