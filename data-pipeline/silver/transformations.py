from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

REQUIRED_FIELDS = [
    "event_id", "event_type", "aggregate_type", 
    "aggregate_id", "occurred_at", "payload", "ingestion_timestamp"
]

def validate_bronze_records(df: DataFrame) -> DataFrame:
    """
    Validates records based on required fields and timestamp parsability.
    Adds a 'quarantine_reason' column. If valid, 'quarantine_reason' is NULL.
    """
    import functools
    
    # Check for nulls in required fields
    null_conditions = [F.col(c).isNull() for c in REQUIRED_FIELDS]
    is_missing_required = functools.reduce(lambda x, y: x | y, null_conditions)

    # Check if occurred_at is a valid timestamp string (simple check: cast to timestamp)
    is_invalid_timestamp = F.to_timestamp(F.col("occurred_at")).isNull()

    df_validated = df.withColumn(
        "quarantine_reason",
        F.when(is_missing_required, F.lit("MISSING_REQUIRED_FIELD"))
         .when(is_invalid_timestamp, F.lit("INVALID_TIMESTAMP"))
         .otherwise(F.lit(None))
    )
    return df_validated

def deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Deduplicates events by event_id.
    Technical tie-breaker used: ordering by ingestion_timestamp ASC, kafka_partition ASC, kafka_offset ASC.
    This ensures deterministic deduplication keeping the earliest ingested record.
    """
    window_spec = Window.partitionBy("event_id").orderBy(
        F.col("ingestion_timestamp").asc(),
        F.col("kafka_partition").asc(),
        F.col("kafka_offset").asc()
    )
    
    # Add row number and filter
    df_deduped = df.withColumn("rn", F.row_number().over(window_spec)) \
                   .filter(F.col("rn") == 1) \
                   .drop("rn")
                   
    return df_deduped

def extract_valid_records(df: DataFrame) -> DataFrame:
    """Filters out invalid records and removes quarantine_reason."""
    return df.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")

def extract_quarantine_records(df: DataFrame) -> DataFrame:
    """Filters only invalid records to be quarantined."""
    return df.filter(F.col("quarantine_reason").isNotNull())

def add_partition_columns(df: DataFrame) -> DataFrame:
    """Adds ingestion_date and hour for Silver writing."""
    return df.withColumn("ingestion_date", F.to_date(F.col("ingestion_timestamp"))) \
             .withColumn("hour", F.date_format(F.col("ingestion_timestamp"), "HH"))
