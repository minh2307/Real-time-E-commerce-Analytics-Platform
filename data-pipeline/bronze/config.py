import os
from dataclasses import dataclass
from pathlib import Path

@dataclass
class BronzeConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    bronze_output_dir: Path
    batch_size: int
    flush_interval_seconds: int

def load_config() -> BronzeConfig:
    return BronzeConfig(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "ecommerce.events"),
        kafka_group_id=os.getenv("KAFKA_GROUP_ID", "bronze-ingestion"),
        bronze_output_dir=Path(os.getenv("BRONZE_OUTPUT_DIR", "data-pipeline/data/bronze")),
        batch_size=int(os.getenv("BRONZE_BATCH_SIZE", "100")),
        flush_interval_seconds=int(os.getenv("BRONZE_FLUSH_INTERVAL_SECONDS", "5"))
    )
