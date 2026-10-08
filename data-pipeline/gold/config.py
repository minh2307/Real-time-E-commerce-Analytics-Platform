import os
from dataclasses import dataclass

@dataclass
class GoldConfig:
    silver_input_dir: str
    gold_output_dir: str

def load_config() -> GoldConfig:
    return GoldConfig(
        silver_input_dir=os.getenv("SILVER_INPUT_DIR", "data-pipeline/data/silver"),
        gold_output_dir=os.getenv("GOLD_OUTPUT_DIR", "data-pipeline/data/gold")
    )
