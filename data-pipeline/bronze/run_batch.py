import logging
import sys
import time
from bronze.config import load_config
from bronze.writer import BronzeParquetWriter
from bronze.consumer import BronzeConsumer

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
    logger.info(f"Starting Bronze Ingestion (Batch Mode) with config: {config}")

    writer = BronzeParquetWriter(config.bronze_output_dir)
    consumer = BronzeConsumer(config, writer)

    consumer.consumer.subscribe([config.kafka_topic])

    buffer = []
    messages_to_commit = []
    last_flush_time = time.time()

    empty_polls = 0
    max_empty_polls = 5 # 5 seconds of no messages = assume batch is done

    try:
        while empty_polls < max_empty_polls:
            msg = consumer.consumer.poll(timeout=1.0)

            if msg is None:
                empty_polls += 1
                continue

            empty_polls = 0

            if msg.error():
                from confluent_kafka import KafkaError, KafkaException
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            event = consumer._parse_message(msg)
            if event:
                buffer.append(event)
                messages_to_commit.append(msg)

            current_time = time.time()
            if len(buffer) >= config.batch_size or (buffer and (current_time - last_flush_time) >= config.flush_interval_seconds):
                consumer._flush(buffer, messages_to_commit)
                last_flush_time = current_time
                buffer.clear()
                messages_to_commit.clear()

        if buffer:
            consumer._flush(buffer, messages_to_commit)

        logger.info("Batch ingestion finished successfully.")

    except Exception as e:
        logger.error(f"Error during batch ingestion: {e}", exc_info=True)
        sys.exit(1)
    finally:
        consumer.consumer.close()

if __name__ == "__main__":
    main()
