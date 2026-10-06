import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import logging
from collections import defaultdict
from pathlib import Path
from typing import List

from .models import BronzeEvent

logger = logging.getLogger(__name__)

class BronzeParquetWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.schema = pa.schema([
            ('event_id', pa.string()),
            ('event_type', pa.string()),
            ('aggregate_type', pa.string()),
            ('aggregate_id', pa.string()),
            ('occurred_at', pa.string()),
            ('payload', pa.string()),
            ('ingestion_timestamp', pa.timestamp('us', tz='UTC')),
            ('kafka_topic', pa.string()),
            ('kafka_partition', pa.int32()),
            ('kafka_offset', pa.int64()),
        ])

    def write_batch(self, events: List[BronzeEvent]) -> None:
        if not events:
            return

        # Group events by partition
        partitions = defaultdict(list)
        for event in events:
            # We partition by ingestion_timestamp, which is UTC
            ingestion_date = event.ingestion_timestamp.strftime("%Y-%m-%d")
            hour = event.ingestion_timestamp.strftime("%H")
            partitions[(ingestion_date, hour)].append(event)

        for (ingestion_date, hour), batch_events in partitions.items():
            self._write_partition(ingestion_date, hour, batch_events)

    def _write_partition(self, ingestion_date: str, hour: str, events: List[BronzeEvent]) -> None:
        partition_dir = self.output_dir / f"ingestion_date={ingestion_date}" / f"hour={hour}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Collision-safe filename
        file_name = f"part-{events[0].ingestion_timestamp.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}.parquet"
        file_path = partition_dir / file_name

        arrays = [
            pa.array([e.event_id for e in events]),
            pa.array([e.event_type for e in events]),
            pa.array([e.aggregate_type for e in events]),
            pa.array([e.aggregate_id for e in events]),
            pa.array([e.occurred_at for e in events]),
            pa.array([e.payload for e in events]),
            pa.array([e.ingestion_timestamp for e in events], type=pa.timestamp('us', tz='UTC')),
            pa.array([e.kafka_topic for e in events]),
            pa.array([e.kafka_partition for e in events]),
            pa.array([e.kafka_offset for e in events]),
        ]

        batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)
        table = pa.Table.from_batches([batch])

        pq.write_table(table, file_path)
        logger.info(f"Wrote {len(events)} events to {file_path}")
