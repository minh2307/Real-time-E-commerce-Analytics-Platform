import os
import shutil
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import json


from gold.dimensions import build_dim_customers, build_dim_categories, build_dim_products
from gold.facts import build_fact_orders_updates, build_fact_order_items, build_fact_payments, build_fact_product_views
from gold.merge import merge_dimension, merge_fact_orders, append_fact
from gold.job import run_gold_job

@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("gold-test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()

@pytest.fixture
def temp_dir(tmpdir):
    os.environ["GOLD_OUTPUT_DIR"] = str(tmpdir)
    return str(tmpdir)

def create_silver_event(spark, event_type, aggregate_type, aggregate_id, payload_dict, occurred_at="2026-09-26T14:30:00Z"):
    payload_str = json.dumps(payload_dict)
    data = [(
        "evt-123", event_type, aggregate_type, aggregate_id, occurred_at, 
        payload_str, datetime(2026, 9, 26, 14, 30, 0), "t", 0, 0
    )]
    schema = StructType([
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
    return spark.createDataFrame(data, schema)

def test_dimensions_creation_and_surrogate_key_determinism(spark):
    payload_c = {"category_id": "cat1", "name": "Cat 1", "slug": "cat1", "parent_category_id": None, "description": "desc"}
    df_cat = create_silver_event(spark, "CATEGORY_CREATED", "CATEGORY", "cat1", payload_c)
    
    payload_p = {"product_id": "prod1", "sku": "SKU1", "title": "Prod 1", "description": "desc", "category_id": "cat1", "current_price": 10.0, "is_active": True}
    df_prod = create_silver_event(spark, "PRODUCT_CREATED", "PRODUCT", "prod1", payload_p)
    
    payload_u = {"user_id": "user1", "email": "a@b.com", "username": "u1", "full_name": "U 1", "role": "CUSTOMER", "status": "ACTIVE"}
    df_user = create_silver_event(spark, "USER_REGISTERED", "USER", "user1", payload_u)
    
    dim_cat_1 = build_dim_categories(df_cat)
    dim_prod_1 = build_dim_products(df_prod, dim_cat_1)
    dim_user_1 = build_dim_customers(df_user)
    
    assert dim_cat_1.count() == 1
    assert dim_prod_1.count() == 1
    assert dim_user_1.count() == 1
    
    key1 = dim_cat_1.collect()[0]["category_key"]
    
    # Run again
    dim_cat_2 = build_dim_categories(df_cat)
    key2 = dim_cat_2.collect()[0]["category_key"]
    
    assert key1 == key2, "Surrogate key must be deterministic"

def test_unknown_member_resolution(spark):
    # Fact order items resolving a product that isn't in dimensions
    payload_o = {
        "order_id": "ord1", "customer_id": "user_unknown", "total_amount": 100.0,
        "subtotal_amount": 100.0, "tax_amount": 0.0, "shipping_amount": 0.0,
        "items": [
            {"order_item_id": "item1", "product_id": "prod_unknown", "quantity": 1, "unit_price": 100.0, "product_sku": "S", "product_title": "T"}
        ]
    }
    df_ord = create_silver_event(spark, "ORDER_CREATED", "ORDER", "ord1", payload_o)
    
    dim_prod = spark.createDataFrame([], schema=StructType([StructField("product_id", StringType(), True), StructField("product_key", LongType(), True)]))
    
    fact_items = build_fact_order_items(df_ord, dim_prod)
    row = fact_items.collect()[0]
    
    assert row["product_key"] == -1, "Unknown member must be resolved to -1"

def test_fact_orders_one_row_per_order_and_idempotency(spark, temp_dir):
    payload_o1 = {"order_id": "ord1", "customer_id": "u1", "total_amount": 100.0, "subtotal_amount": 100.0, "tax_amount": 0.0, "shipping_amount": 0.0, "items": []}
    df_create = create_silver_event(spark, "ORDER_CREATED", "ORDER", "ord1", payload_o1)
    
    dim_cust = spark.createDataFrame([("u1", 123)], ["customer_id", "customer_key"])
    
    updates = build_fact_orders_updates(df_create, dim_cust)
    merge_fact_orders(spark, updates, temp_dir)
    
    res1 = spark.read.parquet(temp_dir)
    assert res1.count() == 1
    assert res1.collect()[0]["is_paid"] == False
    assert res1.collect()[0]["gmv_amount"] == 0.0
    
    # Rerun idempotency
    merge_fact_orders(spark, updates, temp_dir)
    res2 = spark.read.parquet(temp_dir)
    assert res2.count() == 1, "Must be idempotent"
    
    # Now ORDER_PAID arrives late
    payload_o2 = {"order_id": "ord1", "customer_id": "u1", "total_amount": 100.0, "subtotal_amount": 100.0, "tax_amount": 0.0, "shipping_amount": 0.0, "items": []}
    df_paid = create_silver_event(spark, "ORDER_PAID", "ORDER", "ord1", payload_o2)
    
    updates2 = build_fact_orders_updates(df_paid, dim_cust)
    merge_fact_orders(spark, updates2, temp_dir)
    
    res3 = spark.read.parquet(temp_dir)
    assert res3.count() == 1, "Must be exactly 1 row per order"
    assert res3.collect()[0]["is_paid"] == True
    assert res3.collect()[0]["gmv_amount"] == 100.0
    
def test_payment_completed_does_not_create_gmv(spark, temp_dir):
    payload_o1 = {"order_id": "ord2", "customer_id": "u1", "total_amount": 100.0, "subtotal_amount": 100.0, "tax_amount": 0.0, "shipping_amount": 0.0, "items": []}
    df_create = create_silver_event(spark, "ORDER_CREATED", "ORDER", "ord2", payload_o1)
    
    dim_cust = spark.createDataFrame([("u1", 123)], ["customer_id", "customer_key"])
    updates = build_fact_orders_updates(df_create, dim_cust)
    merge_fact_orders(spark, updates, temp_dir)
    
    # PAYMENT_COMPLETED event
    payload_p = {"payment_id": "p1", "order_id": "ord2", "customer_id": "u1", "attempt_number": 1, "status": "SUCCESS", "amount": 100.0, "currency": "USD", "payment_method": "CREDIT_CARD", "error_code": None}
    df_pay = create_silver_event(spark, "PAYMENT_COMPLETED", "PAYMENT", "p1", payload_p)
    
    # Run Gold Job simulation for PAYMENT_COMPLETED
    # Notice it is ignored by fact_orders updates
    updates_pay = build_fact_orders_updates(df_pay, dim_cust)
    merge_fact_orders(spark, updates_pay, temp_dir)
    
    res = spark.read.parquet(temp_dir).filter("order_id = 'ord2'")
    assert res.count() == 1
    assert res.collect()[0]["is_paid"] == False, "PAYMENT_COMPLETED must not set is_paid"
    assert res.collect()[0]["gmv_amount"] == 0.0, "PAYMENT_COMPLETED must not create GMV"

def test_order_item_price_snapshot(spark):
    payload_o = {
        "order_id": "ord3", "customer_id": "u1", "total_amount": 120.0,
        "subtotal_amount": 120.0, "tax_amount": 0.0, "shipping_amount": 0.0,
        "items": [
            {"order_item_id": "item1", "product_id": "prod1", "quantity": 1, "unit_price": 120.0, "product_sku": "SKU1", "product_title": "T1"}
        ]
    }
    df_ord = create_silver_event(spark, "ORDER_CREATED", "ORDER", "ord3", payload_o)
    
    # Product dimension current price is 150
    dim_prod = spark.createDataFrame([("prod1", 456, 150.0)], ["product_id", "product_key", "current_price"])
    
    fact_items = build_fact_order_items(df_ord, dim_prod)
    
    # Should use the snapshot price 120.0, not the current dimension price 150.0
    res = fact_items.collect()[0]
    assert res["unit_price_snapshot"] == 120.0

def test_multiple_payment_attempts(spark, temp_dir):
    payload_p1 = {"payment_id": "p1", "order_id": "ord4", "customer_id": "u1", "attempt_number": 1, "status": "FAILED", "amount": 50.0, "currency": "USD", "payment_method": "CC", "error_code": "ERR"}
    df_p1 = create_silver_event(spark, "PAYMENT_FAILED", "PAYMENT", "p1", payload_p1)
    
    payload_p2 = {"payment_id": "p2", "order_id": "ord4", "customer_id": "u1", "attempt_number": 2, "status": "FAILED", "amount": 50.0, "currency": "USD", "payment_method": "CC", "error_code": "ERR"}
    df_p2 = create_silver_event(spark, "PAYMENT_FAILED", "PAYMENT", "p2", payload_p2)
    
    payload_p3 = {"payment_id": "p3", "order_id": "ord4", "customer_id": "u1", "attempt_number": 3, "status": "SUCCESS", "amount": 50.0, "currency": "USD", "payment_method": "CC", "error_code": None}
    df_p3 = create_silver_event(spark, "PAYMENT_COMPLETED", "PAYMENT", "p3", payload_p3)
    
    df_all = df_p1.union(df_p2).union(df_p3)
    
    dim_cust = spark.createDataFrame([("u1", 123)], ["customer_id", "customer_key"])
    facts = build_fact_payments(df_all, dim_cust)
    append_fact(spark, facts, temp_dir)
    
    res = spark.read.parquet(temp_dir)
    assert res.count() == 3, "All 3 payment attempts must be recorded"
    
def test_product_views(spark, temp_dir):
    # Two identical product views from the same session
    payload_v1 = {"product_id": "prod1", "session_token": "s1", "customer_id": "u1"}
    df_v1 = create_silver_event(spark, "PRODUCT_VIEWED", "VIEW", "v1", payload_v1)
    
    # Change event_id to pretend it's a separate legitimate view event
    payload_v2 = {"product_id": "prod1", "session_token": "s1", "customer_id": "u1"}
    df_v2 = create_silver_event(spark, "PRODUCT_VIEWED", "VIEW", "v2", payload_v2)
    
    df_all = df_v1.union(df_v2)
    dim_cust = spark.createDataFrame([("u1", 123)], ["customer_id", "customer_key"])
    dim_prod = spark.createDataFrame([("prod1", 456)], ["product_id", "product_key"])
    
    facts = build_fact_product_views(df_all, dim_cust, dim_prod)
    append_fact(spark, facts, temp_dir)
    
    res = spark.read.parquet(temp_dir)
    assert res.count() == 2, "Product views must not deduplicate legitimate distinct events"

