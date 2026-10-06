import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import List

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from .config import BronzeConfig
from .models import BronzeEvent
from .writer import BronzeParquetWriter

logger = logging.getLogger(__name__)

class BronzeConsumer:
    def __init__(self, config: BronzeConfig, writer: BronzeParquetWriter):
        self.config = config
        self.writer = writer
        self.consumer = Consumer({
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'group.id': config.kafka_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        self.running = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, sig, frame):
        logger.info(f"Received shutdown signal {sig}, initiating graceful shutdown...")
        self.running = False

    def start(self):
        self.consumer.subscribe([self.config.kafka_topic])
        self.running = True
        logger.info(f"Bronze Consumer started, subscribed to {self.config.kafka_topic}")

        buffer: List[BronzeEvent] = []
        last_flush_time = time.time()
        messages_to_commit: List[Message] = []

        try:
            while self.running:
                # Poll with 1.0s timeout to allow checking shutdown flag and time
                msg = self.consumer.poll(timeout=1.0)

                if msg is not None:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event
                            continue
                        else:
                            raise KafkaException(msg.error())

                    event = self._parse_message(msg)
                    if event:
                        buffer.append(event)
                        messages_to_commit.append(msg)

                current_time = time.time()
                time_since_flush = current_time - last_flush_time

                if len(buffer) >= self.config.batch_size or (buffer and time_since_flush >= self.config.flush_interval_seconds):
                    self._flush(buffer, messages_to_commit)
                    last_flush_time = current_time
                    buffer.clear()
                    messages_to_commit.clear()

        except Exception as e:
            logger.error(f"Consumer encountered an error: {e}", exc_info=True)
        finally:
            # Attempt a final flush if gracefully shutting down
            if buffer:
                logger.info("Flushing remaining buffered events before shutdown...")
                try:
                    self._flush(buffer, messages_to_commit)
                except Exception as e:
                    logger.error("Final flush failed", exc_info=True)

            self.consumer.close()
            logger.info("Bronze Consumer shutdown complete")

    def _parse_message(self, msg: Message) -> BronzeEvent | None:
        try:
            raw_value = msg.value().decode('utf-8')
            data = json.loads(raw_value)

            # payload might be sent as string or object in envelope
            payload = data.get('payload', '')
            if isinstance(payload, dict):
                payload = json.dumps(payload)

            return BronzeEvent(
                event_id=data['event_id'],
                event_type=data['event_type'],
                aggregate_type=data['aggregate_type'],
                aggregate_id=data['aggregate_id'],
                occurred_at=data['occurred_at'],
                payload=payload,
                ingestion_timestamp=datetime.now(timezone.utc),
                kafka_topic=msg.topic(),
                kafka_partition=msg.partition(),
                kafka_offset=msg.offset()
            )
        except Exception as e:
            logger.error(f"Malformed message skipped (offset {msg.offset()}): {e} | value: {msg.value()}")
            # In a full system, we might route this to a quarantine topic/folder
            return None

    def _flush(self, buffer: List[BronzeEvent], messages: List[Message]):
        try:
            self.writer.write_batch(buffer)
            # Only commit if write was successful
            self.consumer.commit(asynchronous=False)
            logger.info(f"Flushed batch of {len(buffer)} events and committed offsets")
        except Exception as e:
            logger.error(f"Failed to flush batch of {len(buffer)} events: {e}")
            raise  # Rethrowing lets the main loop catch and stop, avoiding offset commit
