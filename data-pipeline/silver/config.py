import os
from dataclasses import dataclass

@dataclass
class SilverConfig:
    bronze_input_dir: str
    silver_output_dir: str
    quarantine_output_dir: str

def load_config() -> SilverConfig:
    return SilverConfig(
        bronze_input_dir=os.getenv("BRONZE_INPUT_DIR", "data-pipeline/data/bronze"),
        silver_output_dir=os.getenv("SILVER_OUTPUT_DIR", "data-pipeline/data/silver"),
        quarantine_output_dir=os.getenv("QUARANTINE_OUTPUT_DIR", "data-pipeline/data/quarantine")
    )
