import logging
import sys
from .config import load_config
from .job import create_spark_session, run_silver_job

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    config = load_config()
    logger.info(f"Starting Silver Job with config: {config}")
    
    spark = create_spark_session()
    try:
        run_silver_job(config, spark)
    except Exception as e:
        logger.error(f"Silver job failed: {e}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
