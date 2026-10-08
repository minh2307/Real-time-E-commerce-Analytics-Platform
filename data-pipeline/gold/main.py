import sys
from pyspark.sql import SparkSession
from gold.job import run_gold_job

def main():
    spark = (
        SparkSession.builder
        .appName("ecommerce-gold-pipeline")
        .getOrCreate()
    )
    
    try:
        print("Starting Gold Pipeline...")
        run_gold_job(spark)
        print("Gold Pipeline finished successfully.")
    except Exception as e:
        print(f"Gold Pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
