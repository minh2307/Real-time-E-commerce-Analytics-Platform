import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json
from gold.job import run_gold_job

def create_silver_event(spark, event_type, aggregate_type, aggregate_id, payload_dict, occurred_at="2026-09-26T14:30:00Z"):
    payload_str = json.dumps(payload_dict)
    data = [(
        "evt-"+str(abs(hash(payload_str)))[:5], event_type, aggregate_type, aggregate_id, occurred_at, 
        payload_str, occurred_at, "t", 0, 0
    )]
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("aggregate_type", StringType(), True),
        StructField("aggregate_id", StringType(), True),
        StructField("occurred_at", StringType(), True),
        StructField("payload", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True)
    ])
    return spark.createDataFrame(data, schema)

def main():
    print("--- STARTING GOLD INTEGRATION VALIDATION ---")
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("gold-integration")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    
    # 1. Create Silver mock data
    
    # Dimension events
    c_df = create_silver_event(spark, "USER_REGISTERED", "USER", "c1", {"user_id": "c1", "email": "a@b", "username": "u", "full_name": "N", "role": "C", "status": "A"})
    p_df = create_silver_event(spark, "PRODUCT_CREATED", "PRODUCT", "p1", {"product_id": "p1", "sku": "S1", "title": "T1", "description": "D", "category_id": "cat1", "current_price": 100.0, "is_active": True})
    cat_df = create_silver_event(spark, "CATEGORY_CREATED", "CATEGORY", "cat1", {"category_id": "cat1", "name": "C1", "slug": "c1", "parent_category_id": None, "description": "D"})
    
    # Order 1: Created -> Paid
    o1_c = create_silver_event(spark, "ORDER_CREATED", "ORDER", "ord1", {
        "order_id": "ord1", "customer_id": "c1", "total_amount": 250.0, "subtotal_amount": 250.0, "tax_amount": 0.0, "shipping_amount": 0.0,
        "items": [
            {"order_item_id": "i1", "product_id": "p1", "quantity": 2, "unit_price": 125.0, "product_sku": "S1", "product_title": "T1"}
        ]
    })
    o1_p = create_silver_event(spark, "ORDER_PAID", "ORDER", "ord1", {
        "order_id": "ord1", "customer_id": "c1", "total_amount": 250.0, "subtotal_amount": 250.0, "tax_amount": 0.0, "shipping_amount": 0.0, "items": []
    })
    
    # Order 2: Created -> Payment Completed (NOT PAID YET)
    o2_c = create_silver_event(spark, "ORDER_CREATED", "ORDER", "ord2", {
        "order_id": "ord2", "customer_id": "c1", "total_amount": 50.0, "subtotal_amount": 50.0, "tax_amount": 0.0, "shipping_amount": 0.0, "items": []
    })
    pay_c = create_silver_event(spark, "PAYMENT_COMPLETED", "PAYMENT", "pay1", {
        "payment_id": "pay1", "order_id": "ord2", "customer_id": "c1", "attempt_number": 1, "status": "SUCCESS", "amount": 50.0, "currency": "USD", "payment_method": "CC", "error_code": None
    })
    
    silver_df = c_df.union(p_df).union(cat_df).union(o1_c).union(o1_p).union(o2_c).union(pay_c)
    
    # Run Gold Job
    out_dir = "/tmp/gold_test_out"
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.environ["GOLD_OUTPUT_DIR"] = out_dir
    
    print("--- RUN 1 ---")
    run_gold_job(spark, silver_df)
    
    # Verify Run 1
    dim_cust = spark.read.parquet(f"{out_dir}/dimensions/dim_customers")
    dim_prod = spark.read.parquet(f"{out_dir}/dimensions/dim_products")
    dim_cat = spark.read.parquet(f"{out_dir}/dimensions/dim_categories")
    
    fact_orders = spark.read.parquet(f"{out_dir}/facts/fact_orders")
    fact_items = spark.read.parquet(f"{out_dir}/facts/fact_order_items")
    fact_payments = spark.read.parquet(f"{out_dir}/facts/fact_payments")
    
    assert dim_cust.count() == 1
    assert dim_prod.count() == 1
    assert dim_cat.count() == 1
    
    assert fact_orders.count() == 2
    assert fact_items.count() == 1
    assert fact_payments.count() == 1
    
    # Verify GMV rules
    o1_row = fact_orders.filter("order_id = 'ord1'").collect()[0]
    o2_row = fact_orders.filter("order_id = 'ord2'").collect()[0]
    
    assert o1_row["is_paid"] == True
    assert o1_row["gmv_amount"] == 250.0
    
    assert o2_row["is_paid"] == False, "PAYMENT_COMPLETED must not set is_paid"
    assert o2_row["gmv_amount"] == 0.0, "PAYMENT_COMPLETED must not set GMV"
    
    print("--- RUN 2 (IDEMPOTENCY) ---")
    run_gold_job(spark, silver_df)
    
    fact_orders_run2 = spark.read.parquet(f"{out_dir}/facts/fact_orders")
    assert fact_orders_run2.count() == 2, "fact_orders must be idempotent"
    
    print("--- RUN 3 (LATE ARRIVING DATA) ---")
    # Now ORDER_PAID arrives for ord2
    o2_p = create_silver_event(spark, "ORDER_PAID", "ORDER", "ord2", {
        "order_id": "ord2", "customer_id": "c1", "total_amount": 50.0, "subtotal_amount": 50.0, "tax_amount": 0.0, "shipping_amount": 0.0, "items": []
    })
    # We must format timestamp properly for PySpark
    run_gold_job(spark, o2_p)
    
    fact_orders_run3 = spark.read.parquet(f"{out_dir}/facts/fact_orders")
    assert fact_orders_run3.count() == 2, "fact_orders must not fan out"
    o2_row_new = fact_orders_run3.filter("order_id = 'ord2'").collect()[0]
    assert o2_row_new["is_paid"] == True
    assert o2_row_new["gmv_amount"] == 50.0, "Late ORDER_PAID must trigger GMV"
    
    print("ALL INTEGRATION VALIDATIONS PASSED SUCCESSFULLY")
    
if __name__ == "__main__":
    main()
