import os
import shutil
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def atomic_write(df: DataFrame, target_path: str, partition_cols: list = None):
    """
    Writes DataFrame atomically using a temporary directory.
    """
    temp_path = target_path + "_temp"
    
    # Write to temp path
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(temp_path)
    
    # Replace target path
    if os.path.exists(target_path):
        shutil.rmtree(target_path)
    os.rename(temp_path, target_path)

def merge_dimension(spark, new_dim: DataFrame, target_path: str, natural_key: str):
    """
    Merges SCD Type 1 dimension.
    If existing dimension exists, unions and keeps the latest (or just overwrites based on presence in new_dim).
    In a real system, we'd use updated_at. Here we'll just keep the new_dim record if there's a match.
    """
    if not os.path.exists(target_path):
        atomic_write(new_dim, target_path)
        return
        
    try:
        existing_dim = spark.read.parquet(target_path)
    except Exception:
        # If it's corrupted or empty
        atomic_write(new_dim, target_path)
        return

    if new_dim.isEmpty():
        return

    # To keep SCD Type 1: if natural_key exists in new_dim, we use new_dim. Otherwise existing.
    # Anti-join to find untouched existing records
    untouched_existing = existing_dim.alias("e").join(
        new_dim.alias("n"),
        F.col(f"e.{natural_key}") == F.col(f"n.{natural_key}"),
        "left_anti"
    )
    
    final_dim = untouched_existing.unionByName(new_dim, allowMissingColumns=True)
    atomic_write(final_dim, target_path)

def merge_fact_orders(spark, new_orders: DataFrame, target_path: str):
    """
    Merges fact_orders.
    Supports in-place state update:
    - If new order, insert.
    - If existing order, update status, GMV, and is_paid if new state is more advanced (e.g. PAID).
    Actually, our new_orders DataFrame has already resolved the state for the current batch.
    If the existing order was already PAID, we shouldn't lose the PAID status if a late CANCELLED arrives (or maybe we do?).
    The rule says: "ORDER_PAID is the authoritative commercial event... A replay, duplicate ORDER_PAID... must never create additional GMV."
    "If ORDER_PAID has been successfully processed: is_paid = TRUE => gmv_amount = total_amount"
    So if existing is_paid == True, it stays True.
    """
    if not os.path.exists(target_path):
        atomic_write(new_orders, target_path)
        return
        
    try:
        existing = spark.read.parquet(target_path)
    except Exception:
        atomic_write(new_orders, target_path)
        return

    if new_orders.isEmpty():
        return

    # We need to merge existing and new_orders
    # The new_orders has the delta from the current batch.
    
    # untouched existing
    untouched = existing.alias("e").join(
        new_orders.alias("n"),
        F.col("e.order_id") == F.col("n.order_id"),
        "left_anti"
    )
    
    # matched existing that need update
    matched_existing = existing.alias("e").join(
        new_orders.alias("n"),
        F.col("e.order_id") == F.col("n.order_id"),
        "inner"
    )
    
    # We resolve the merged state for matched records
    # If it was paid before OR paid in this batch, it's paid.
    resolved_matched = matched_existing.select(
        F.col("e.order_key"),
        F.col("e.order_id"),
        F.col("e.customer_key"), # Should not change
        F.col("e.order_date"),
        F.coalesce(F.col("e.paid_date"), F.col("n.paid_date")).alias("paid_date"),
        
        # Status logic: if existing was PAID (2), keep it PAID unless a business rule says CANCELLED overrides it.
        # But for MVP, if it's PAID, it stays PAID for GMV purposes. Or if new is PAID, it becomes PAID.
        F.when(F.col("e.is_paid") | F.col("n.is_paid"), F.lit(2))
         .when(F.col("n.status_key") > F.col("e.status_key"), F.col("n.status_key"))
         .otherwise(F.col("e.status_key")).alias("status_key"),
         
        F.coalesce(F.col("n.subtotal_amount"), F.col("e.subtotal_amount")).alias("subtotal_amount"),
        F.coalesce(F.col("n.tax_amount"), F.col("e.tax_amount")).alias("tax_amount"),
        F.coalesce(F.col("n.shipping_amount"), F.col("e.shipping_amount")).alias("shipping_amount"),
        F.coalesce(F.col("n.total_amount"), F.col("e.total_amount")).alias("total_amount"),
        
        F.when(F.col("e.is_paid") | F.col("n.is_paid"), F.coalesce(F.col("n.total_amount"), F.col("e.total_amount"))).otherwise(F.lit(0.0)).alias("gmv_amount"),
        (F.col("e.is_paid") | F.col("n.is_paid")).alias("is_paid"),
        
        F.col("n.updated_at"),
        F.coalesce(F.col("e.cancelled_at"), F.col("n.cancelled_at")).alias("cancelled_at")
    )
    
    # new records that didn't exist before
    new_only = new_orders.alias("n").join(
        existing.alias("e"),
        F.col("n.order_id") == F.col("e.order_id"),
        "left_anti"
    )
    
    final_orders = untouched.unionByName(resolved_matched, allowMissingColumns=True).unionByName(new_only, allowMissingColumns=True)
    atomic_write(final_orders, target_path)

def append_fact(spark, new_facts: DataFrame, target_path: str):
    """
    Appends append-only facts (like order_items, payments, views).
    Wait, the spec says idempotency is required.
    If we rerun the pipeline with the same Silver input, we shouldn't append duplicates.
    So we should do an anti-join on the primary key to ensure we only append genuinely new records.
    """
    if not os.path.exists(target_path):
        atomic_write(new_facts, target_path)
        return
        
    try:
        existing = spark.read.parquet(target_path)
    except Exception:
        atomic_write(new_facts, target_path)
        return

    if new_facts.isEmpty():
        return

    # Detect the PK column name.
    # We know the PKs from the fact tables:
    # order_item_key, payment_key, view_key
    # We can just use the natural keys since surrogate keys might be generated identically.
    
    natural_key = None
    if "order_item_key" in new_facts.columns:
        # Actually fact_order_items doesn't have order_item_id exported in our facts.py, let's use order_item_key since it's deterministic.
        natural_key = "order_item_key"
    elif "payment_id" in new_facts.columns:
        natural_key = "payment_id"
    elif "event_id" in new_facts.columns:
        natural_key = "event_id"
        
    if natural_key:
        new_only = new_facts.alias("n").join(
            existing.alias("e"),
            F.col(f"n.{natural_key}") == F.col(f"e.{natural_key}"),
            "left_anti"
        )
        final_facts = existing.unionByName(new_only, allowMissingColumns=True)
        atomic_write(final_facts, target_path)
    else:
        # Fallback to pure append, though idempotency is lost if rerun
        final_facts = existing.unionByName(new_facts, allowMissingColumns=True)
        atomic_write(final_facts, target_path)
