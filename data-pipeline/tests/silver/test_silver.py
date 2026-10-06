import pytest
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pathlib import Path

from silver.config import SilverConfig
from silver.schemas import BRONZE_SCHEMA
from silver.job import run_silver_job

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("SilverTest") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def config(tmp_path):
    return SilverConfig(
        bronze_input_dir=str(tmp_path / "bronze"),
        silver_output_dir=str(tmp_path / "silver"),
        quarantine_output_dir=str(tmp_path / "quarantine")
    )

def create_bronze_df(spark, rows):
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

def write_bronze(df, path):
    df.write.mode("overwrite").parquet(path)

def test_1_valid_records(spark, config):
    rows = [
        Row("1", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 0),
        Row("2", "T", "A", "2", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 1),
        Row("3", "T", "A", "3", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 2),
    ]
    df = create_bronze_df(spark, rows)
    write_bronze(df, config.bronze_input_dir)
    
    run_silver_job(config, spark)
    
    silver_df = spark.read.parquet(config.silver_output_dir)
    assert silver_df.count() == 3

def test_2_duplicate_event(spark, config):
    rows = [
        Row("A", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 0),
        Row("A", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 1), "t", 0, 1),
        Row("B", "T", "A", "2", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 2),
    ]
    df = create_bronze_df(spark, rows)
    write_bronze(df, config.bronze_input_dir)
    
    run_silver_job(config, spark)
    
    silver_df = spark.read.parquet(config.silver_output_dir)
    assert silver_df.count() == 2
    event_ids = [r.event_id for r in silver_df.select("event_id").collect()]
    assert sorted(event_ids) == ["A", "B"]

def test_3_deterministic_deduplication(spark, config):
    # event_id A should pick the one with earliest ingestion_timestamp (offset 10)
    rows = [
        Row("A", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 2), "t", 0, 20),
        Row("A", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 1), "t", 0, 10),
        Row("A", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 3), "t", 0, 30),
    ]
    df = create_bronze_df(spark, rows)
    write_bronze(df, config.bronze_input_dir)
    
    run_silver_job(config, spark)
    
    silver_df = spark.read.parquet(config.silver_output_dir)
    res = silver_df.collect()[0]
    assert res.kafka_offset == 10

def test_4_invalid_required_field(spark, config):
    # payload is missing
    rows = [
        Row("1", "T", "A", "1", "2026-10-06T10:00:00Z", None, datetime(2026, 10, 6, 10, 0, 0), "t", 0, 0),
        Row("2", "T", "A", "2", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 1),
    ]
    df = create_bronze_df(spark, rows)
    write_bronze(df, config.bronze_input_dir)
    
    run_silver_job(config, spark)
    
    silver_df = spark.read.parquet(config.silver_output_dir)
    assert silver_df.count() == 1
    
    quarantine_df = spark.read.parquet(config.quarantine_output_dir)
    assert quarantine_df.count() == 1
    assert quarantine_df.collect()[0].quarantine_reason == "MISSING_REQUIRED_FIELD"

def test_5_invalid_timestamp(spark, config):
    rows = [
        Row("1", "T", "A", "1", "invalid_time", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 0),
    ]
    df = create_bronze_df(spark, rows)
    write_bronze(df, config.bronze_input_dir)
    
    run_silver_job(config, spark)
    
    # Silver should be empty, but directory might not exist if empty.
    assert not Path(config.silver_output_dir).exists() or spark.read.parquet(config.silver_output_dir).count() == 0
    
    quarantine_df = spark.read.parquet(config.quarantine_output_dir)
    assert quarantine_df.count() == 1
    assert quarantine_df.collect()[0].quarantine_reason == "INVALID_TIMESTAMP"

def test_7_idempotent_rerun(spark, config):
    rows = [
        Row("1", "T", "A", "1", "2026-10-06T10:00:00Z", "{}", datetime(2026, 10, 6, 10, 0, 0), "t", 0, 0),
    ]
    df = create_bronze_df(spark, rows)
    write_bronze(df, config.bronze_input_dir)
    
    # Run 1
    run_silver_job(config, spark)
    count1 = spark.read.parquet(config.silver_output_dir).count()
    assert count1 == 1
    
    # Run 2
    run_silver_job(config, spark)
    count2 = spark.read.parquet(config.silver_output_dir).count()
    assert count2 == 1  # Should not duplicate
