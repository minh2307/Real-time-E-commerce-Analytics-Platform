from dataclasses import dataclass
from typing import Optional, Any
from datetime import datetime

@dataclass
class BronzeEvent:
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: str
    payload: str
    ingestion_timestamp: datetime
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
