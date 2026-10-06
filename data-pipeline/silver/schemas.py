from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, LongType
)

# Bronze input schema based on TASK 08 requirements
BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("aggregate_type", StringType(), True),
    StructField("aggregate_id", StringType(), True),
    StructField("occurred_at", StringType(), True),
    StructField("payload", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True)
])
