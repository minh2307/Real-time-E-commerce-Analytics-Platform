import logging
import sys
from .config import load_config
from .writer import BronzeParquetWriter
from .consumer import BronzeConsumer

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
    logger.info(f"Starting Bronze Ingestion with config: {config}")

    writer = BronzeParquetWriter(config.bronze_output_dir)
    consumer = BronzeConsumer(config, writer)

    consumer.start()

if __name__ == "__main__":
    main()
