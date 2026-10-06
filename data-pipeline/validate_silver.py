import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import Row

from silver.schemas import BRONZE_SCHEMA
from silver.config import SilverConfig
from silver.job import run_silver_job

def main():
    # Setup test paths
    base_dir = Path(os.getcwd()) / "test_data_validation"
    bronze_dir = base_dir / "bronze"
    silver_dir = base_dir / "silver"
    quarantine_dir = base_dir / "quarantine"
    
    # Cleanup previous runs
    if base_dir.exists():
        shutil.rmtree(base_dir)
        
    config = SilverConfig(
        bronze_input_dir=str(bronze_dir),
        silver_output_dir=str(silver_dir),
        quarantine_output_dir=str(quarantine_dir)
    )
    
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("silver-integration-test") \
        .getOrCreate()
        
    # Create Bronze Data
    # 1 valid event (A)
    # 1 duplicate event (A) (higher offset, should be dropped)
    # 1 valid event (B)
    # 1 invalid required field event (missing occurred_at)
    # 1 invalid timestamp event (invalid occurred_at string)
    
    dt1 = datetime(2026, 10, 6, 10, 0, 0)
    dt2 = datetime(2026, 10, 6, 10, 5, 0)
    
    rows = [
        Row("A", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", dt1, "t", 0, 10), # Valid A
        Row("A", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", dt2, "t", 0, 20), # Duplicate A (later)
        Row("B", "T", "A", "2", "2026-10-06T10:00:00Z", "{}", dt1, "t", 0, 15), # Valid B
        Row("C", "T", "A", "3", None, "{}", dt1, "t", 0, 25), # Invalid missing required
        Row("D", "T", "A", "4", "invalid_date", "{}", dt1, "t", 0, 30), # Invalid timestamp
    ]
    
    df_bronze = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    df_bronze.write.parquet(str(bronze_dir))
    
    print("--- RUNNING SILVER JOB (RUN 1) ---")
    run_silver_job(config, spark)
    
    df_silver = spark.read.parquet(str(silver_dir))
    df_quarantine = spark.read.parquet(str(quarantine_dir))
    
    silver_count = df_silver.count()
    quarantine_count = df_quarantine.count()
    
    print(f"Silver count: {silver_count}")
    print(f"Quarantine count: {quarantine_count}")
    print("Silver records:")
    df_silver.show(truncate=False)
    print("Quarantine records:")
    df_quarantine.show(truncate=False)
    
    assert silver_count == 2, "Expected 2 valid records in silver"
    assert quarantine_count == 2, "Expected 2 invalid records in quarantine"
    
    # Check that A has the earlier offset
    res_a = df_silver.filter(df_silver.event_id == "A").collect()[0]
    assert res_a.kafka_offset == 10, "Deduplication did not pick the earliest record"
    
    # Check partitioning
    partition_folders = list(silver_dir.glob("ingestion_date=*"))
    assert len(partition_folders) > 0, "No partition folders found"
    
    print("--- RUNNING SILVER JOB AGAIN (RUN 2) - TESTING IDEMPOTENCY ---")
    run_silver_job(config, spark)
    
    df_silver2 = spark.read.parquet(str(silver_dir))
    silver_count2 = df_silver2.count()
    print(f"Silver count after Run 2: {silver_count2}")
    
    assert silver_count2 == 2, "Idempotency failed, silver count changed after rerun"
    
    spark.stop()
    print("ALL INTEGRATION VALIDATIONS PASSED")

if __name__ == "__main__":
    main()
