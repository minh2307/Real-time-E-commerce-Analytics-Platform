import os
from pyspark.sql import SparkSession
from gold.config import load_config
from gold.dimensions import build_dim_customers, build_dim_categories, build_dim_products
from gold.facts import (
    build_fact_orders_updates, build_fact_order_items, 
    build_fact_payments, build_fact_product_views
)
from gold.merge import merge_dimension, merge_fact_orders, append_fact

def run_gold_job(spark: SparkSession, silver_df=None):
    """
    Executes the Gold pipeline.
    """
    config = load_config()
    
    if silver_df is None:
        try:
            silver_df = spark.read.parquet(config.silver_input_dir)
        except Exception:
            print("No Silver data found.")
            return
            
    # Cache silver to prevent re-evaluation
    silver_df.cache()
    
    # 1. Dimensions
    # dim_categories
    dim_categories = build_dim_categories(silver_df)
    cat_path = os.path.join(config.gold_output_dir, "dimensions", "dim_categories")
    merge_dimension(spark, dim_categories, cat_path, "category_id")
    
    # Reload from disk to get full historical dimension for joins
    try:
        full_dim_categories = spark.read.parquet(cat_path)
    except:
        full_dim_categories = dim_categories
        
    # dim_products
    dim_products = build_dim_products(silver_df, full_dim_categories)
    prod_path = os.path.join(config.gold_output_dir, "dimensions", "dim_products")
    merge_dimension(spark, dim_products, prod_path, "product_id")
    
    try:
        full_dim_products = spark.read.parquet(prod_path)
    except:
        full_dim_products = dim_products

    # dim_customers
    dim_customers = build_dim_customers(silver_df)
    cust_path = os.path.join(config.gold_output_dir, "dimensions", "dim_customers")
    merge_dimension(spark, dim_customers, cust_path, "customer_id")
    
    try:
        full_dim_customers = spark.read.parquet(cust_path)
    except:
        full_dim_customers = dim_customers
        
    # 2. Facts
    # fact_orders
    fact_orders_updates = build_fact_orders_updates(silver_df, full_dim_customers)
    orders_path = os.path.join(config.gold_output_dir, "facts", "fact_orders")
    merge_fact_orders(spark, fact_orders_updates, orders_path)
    
    # fact_order_items
    fact_order_items = build_fact_order_items(silver_df, full_dim_products)
    items_path = os.path.join(config.gold_output_dir, "facts", "fact_order_items")
    append_fact(spark, fact_order_items, items_path)
    
    # fact_payments
    fact_payments = build_fact_payments(silver_df, full_dim_customers)
    payments_path = os.path.join(config.gold_output_dir, "facts", "fact_payments")
    append_fact(spark, fact_payments, payments_path)
    
    # fact_product_views
    fact_product_views = build_fact_product_views(silver_df, full_dim_customers, full_dim_products)
    views_path = os.path.join(config.gold_output_dir, "facts", "fact_product_views")
    append_fact(spark, fact_product_views, views_path)
    
    print("Gold job completed successfully.")
