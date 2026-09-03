import logging
from pathlib import Path

# --------------------------------------------------
# 1. Create logs directory
# --------------------------------------------------

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "pipeline.log"


# --------------------------------------------------
# 2. Configure logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("pyspark_pipeline")


# --------------------------------------------------
# 3. Pipeline execution
# --------------------------------------------------

logger.info("Pipeline started")

try:
    logger.info("Reading source data")

    # Simulated processing
    records_processed = 96478

    logger.info("Records processed: %s", records_processed)

    logger.info("Writing output data")

    logger.info("Pipeline completed successfully")

except Exception as e:
    logger.error("Pipeline failed: %s", e, exc_info=True)