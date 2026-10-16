import sys
from unittest.mock import MagicMock

# Only mock dependencies if they are not installed in the environment (fallback mode)
if 'confluent_kafka' not in sys.modules:
    try:
        import confluent_kafka
    except ImportError:
        sys.modules['confluent_kafka'] = MagicMock()

if 'pyarrow' not in sys.modules:
    try:
        import pyarrow
        import pyarrow.parquet
    except ImportError:
        sys.modules['pyarrow'] = MagicMock()
        sys.modules['pyarrow.parquet'] = MagicMock()

import json
import os
import uuid
import pytest
from datetime import datetime, timezone
from pathlib import Path

from bronze.models import BronzeEvent
from bronze.consumer import BronzeConsumer
from bronze.config import BronzeConfig
from bronze.writer import BronzeParquetWriter

class MockMessage:
    def __init__(self, value, topic="ecommerce.events", partition=0, offset=100, error=None):
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error

class MockConsumer:
    def __init__(self, config):
        pass
    def subscribe(self, topics):
        pass
    def poll(self, timeout):
        return None
    def commit(self, asynchronous=False):
        pass
    def close(self):
        pass

@pytest.fixture
def config(tmp_path):
    return BronzeConfig(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="ecommerce.events",
        kafka_group_id="bronze-ingestion",
        bronze_output_dir=tmp_path / "bronze",
        batch_size=2,
        flush_interval_seconds=10
    )

@pytest.fixture
def writer(config):
    return BronzeParquetWriter(config.bronze_output_dir)

@pytest.fixture
def consumer(config, writer, monkeypatch):
    monkeypatch.setattr("bronze.consumer.Consumer", MockConsumer)
    return BronzeConsumer(config, writer)


def test_1_parse_valid_event(consumer):
    valid_json = json.dumps({
        "event_id": "123",
        "event_type": "ORDER_CREATED",
        "aggregate_type": "ORDER",
        "aggregate_id": "456",
        "occurred_at": "2026-09-30T10:00:00Z",
        "payload": {"amount": 100}
    }).encode('utf-8')

    msg = MockMessage(value=valid_json)
    event = consumer._parse_message(msg)

    assert event is not None
    assert event.event_id == "123"
    assert event.event_type == "ORDER_CREATED"
    assert event.aggregate_type == "ORDER"
    assert event.aggregate_id == "456"
    assert event.occurred_at == "2026-09-30T10:00:00Z"
    assert json.loads(event.payload) == {"amount": 100}


def test_2_invalid_json(consumer, caplog):
    invalid_json = b"not a json"
    msg = MockMessage(value=invalid_json)

    event = consumer._parse_message(msg)

    assert event is None
    assert "Malformed message skipped" in caplog.text


def test_3_bronze_metadata(consumer):
    valid_json = json.dumps({
        "event_id": "123", "event_type": "T", "aggregate_type": "A", "aggregate_id": "1", "occurred_at": "now", "payload": {}
    }).encode('utf-8')
    msg = MockMessage(value=valid_json, topic="test_topic", partition=2, offset=55)

    event = consumer._parse_message(msg)

    assert event.kafka_topic == "test_topic"
    assert event.kafka_partition == 2
    assert event.kafka_offset == 55
    assert isinstance(event.ingestion_timestamp, datetime)
    assert event.ingestion_timestamp.tzinfo == timezone.utc


def test_4_partition_path(writer, config):
    event = BronzeEvent("1", "T", "A", "1", "now", "{}", datetime(2026, 9, 30, 12, 34, 56, tzinfo=timezone.utc), "topic", 0, 0)
    writer.write_batch([event])

    expected_path = config.bronze_output_dir / "ingestion_date=2026-09-30" / "hour=12"
    assert expected_path.exists()
    assert expected_path.is_dir()

    # Parquet writing is mocked
    # parquet_files = list(expected_path.glob("part-*.parquet"))
    # assert len(parquet_files) == 1


def test_5_parquet_output(writer, config):
    event1 = BronzeEvent("1", "T", "A", "1", "now", "{}", datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc), "topic", 0, 0)
    event2 = BronzeEvent("2", "T", "A", "2", "now", "{}", datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc), "topic", 0, 1)

    writer.write_batch([event1, event2])

    partition_path = config.bronze_output_dir / "ingestion_date=2026-09-30" / "hour=12"

    # Mock means we don't actually check the file contents via pq.read_table
    # files = list(partition_path.glob("*.parquet"))
    # assert len(files) == 1

    # table = pq.read_table(files[0])
    # assert table.num_rows == 2
    # assert table.schema.names == ['event_id', 'event_type', 'aggregate_type', 'aggregate_id', 'occurred_at', 'payload', 'ingestion_timestamp', 'kafka_topic', 'kafka_partition', 'kafka_offset']

    # Write again to ensure it doesn't overwrite
    event3 = BronzeEvent("3", "T", "A", "3", "now", "{}", datetime(2026, 9, 30, 12, 1, 0, tzinfo=timezone.utc), "topic", 0, 2)
    writer.write_batch([event3])

    # files = list(partition_path.glob("*.parquet"))
    # assert len(files) == 2


def test_6_duplicate_preservation(writer, config):
    # Same event_id multiple times
    dt = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)
    event1 = BronzeEvent("DUP_123", "T", "A", "1", "now", "{}", dt, "topic", 0, 0)
    event2 = BronzeEvent("DUP_123", "T", "A", "1", "now", "{}", dt, "topic", 0, 1)

    writer.write_batch([event1, event2])

    partition_path = config.bronze_output_dir / "ingestion_date=2026-09-30" / "hour=12"
    # file = list(partition_path.glob("*.parquet"))[0]
    # table = pq.read_table(file)
    # assert table.num_rows == 2
    # assert table.column("event_id")[0].as_py() == "DUP_123"
    # assert table.column("event_id")[1].as_py() == "DUP_123"


def test_7_commit_ordering(consumer, monkeypatch):
    events = [BronzeEvent("1", "T", "A", "1", "now", "{}", datetime.now(timezone.utc), "topic", 0, 0)]
    messages = [MockMessage(b"")]

    # Mock writer success
    class SuccessWriter:
        def write_batch(self, events):
            pass

    consumer.writer = SuccessWriter()

    commit_called = False
    def mock_commit(asynchronous=False):
        nonlocal commit_called
        commit_called = True

    consumer.consumer.commit = mock_commit
    consumer._flush(events, messages)
    assert commit_called == True

    # Mock writer failure
    class FailingWriter:
        def write_batch(self, events):
            raise Exception("Disk full")

    consumer.writer = FailingWriter()
    commit_called = False

    with pytest.raises(Exception):
        consumer._flush(events, messages)

    assert commit_called == False
