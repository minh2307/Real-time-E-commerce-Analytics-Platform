import os
from dataclasses import dataclass

@dataclass
class AnalyticsConfig:
    gold_output_dir: str
    duckdb_path: str

def load_config() -> AnalyticsConfig:
    return AnalyticsConfig(
        gold_output_dir=os.getenv("GOLD_OUTPUT_DIR", "data-pipeline/data/gold"),
        duckdb_path=os.getenv("DUCKDB_PATH", "data-pipeline/data/analytics.duckdb")
    )
