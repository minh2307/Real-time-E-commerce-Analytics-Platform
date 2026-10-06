import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from .config import SilverConfig
from .schemas import BRONZE_SCHEMA
from .transformations import (
    validate_bronze_records,
    deduplicate_events,
    extract_valid_records,
    extract_quarantine_records,
    add_partition_columns
)

logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("SilverCleansing") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

def run_silver_job(config: SilverConfig, spark: SparkSession):
    try:
        df_bronze = spark.read.schema(BRONZE_SCHEMA).parquet(config.bronze_input_dir)
    except AnalysisException as e:
        logger.warning(f"Failed to read from bronze dir {config.bronze_input_dir}. Maybe empty? Error: {e}")
        return

    if df_bronze.isEmpty():
        logger.info("No bronze records found. Exiting.")
        return
        
    df_validated = validate_bronze_records(df_bronze)
    
    # Split valid and quarantine
    df_valid = extract_valid_records(df_validated)
    df_quarantine = extract_quarantine_records(df_validated)
    
    # Deduplicate valid
    df_deduped = deduplicate_events(df_valid)
    
    # Add partition columns for Silver
    df_silver = add_partition_columns(df_deduped)
    
    # Write quarantine
    if not df_quarantine.isEmpty():
        logger.info("Writing quarantine records...")
        df_quarantine.write \
            .mode("append") \
            .parquet(config.quarantine_output_dir)
            
    # Write silver with idempotency (overwrite partitions)
    # Using mode("overwrite") with partitionBy makes it idempotent for batch runs on specific partitions
    # Note: Dynamic partition overwrite is safer for prod, but overwrite is sufficient here.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    if not df_silver.isEmpty():
        logger.info("Writing silver records...")
        df_silver.write \
            .mode("overwrite") \
            .partitionBy("ingestion_date", "hour") \
            .parquet(config.silver_output_dir)
            
    logger.info("Silver job completed.")
