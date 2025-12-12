import logging
from moata_pipeline.collect.runner import run_collect_rain_gauges

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

if __name__ == "__main__":
    run_collect_rain_gauges()
