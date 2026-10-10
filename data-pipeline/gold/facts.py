from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from gold.schemas import ORDER_PAYLOAD_SCHEMA, PAYMENT_PAYLOAD_SCHEMA, VIEW_PAYLOAD_SCHEMA
from gold.dimensions import generate_surrogate_key

def build_fact_orders_updates(silver_df: DataFrame, dim_customers: DataFrame) -> DataFrame:
    """
    Builds fact_orders updates from the current batch.
    Rule: 1 row / 1 order.
    ONLY ORDER_PAID triggers GMV.
    """
    orders = silver_df.filter(F.col("aggregate_type") == "ORDER")
    if orders.isEmpty():
        # Return empty df with correct schema if possible, or just let caller handle empty
        pass # Will return empty dataframe naturally with the following transforms
        
    parsed = orders.withColumn("data", F.from_json(F.col("payload"), ORDER_PAYLOAD_SCHEMA))
    
    # We want to aggregate by order_id to find the latest state and whether it was paid
    w = Window.partitionBy("data.orderId")
    
    # Check for specific events in the batch for this order
    has_paid = F.max(F.when(F.col("event_type") == "ORDER_PAID", 1).otherwise(0)).over(w)
    has_cancelled = F.max(F.when(F.col("event_type") == "ORDER_CANCELLED", 1).otherwise(0)).over(w)
    
    paid_date = F.max(F.when(F.col("event_type") == "ORDER_PAID", F.to_date(F.col("occurred_at"))).otherwise(F.lit(None))).over(w)
    cancelled_date = F.max(F.when(F.col("event_type") == "ORDER_CANCELLED", F.col("occurred_at")).otherwise(F.lit(None))).over(w)
    
    # Order events by time to get the latest payload for amounts/customer
    w_latest = Window.partitionBy("data.orderId").orderBy(
        F.col("occurred_at").desc(), 
        F.col("kafka_offset").desc()
    )
    
    latest_events = parsed.withColumn("rn", F.row_number().over(w_latest)) \
                          .withColumn("batch_has_paid", has_paid) \
                          .withColumn("batch_has_cancelled", has_cancelled) \
                          .withColumn("batch_paid_date", paid_date) \
                          .withColumn("batch_cancelled_at", cancelled_date) \
                          .filter(F.col("rn") == 1)
                          
    # Resolve customer key
    with_cust = latest_events.alias("o").join(
        dim_customers.alias("c"),
        F.col("o.data.customerId") == F.col("c.customer_id"),
        "left"
    )

    # Determine status and GMV
    # Status logic for MVP based on event presence in this batch. 
    # Note: real merge logic will also look at existing Gold state if it's already PAID.
    # Here we just output the incremental update delta.
    
    df = with_cust.select(
        generate_surrogate_key("o.data.orderId").alias("order_key"),
        F.col("o.data.orderId").alias("order_id"),
        F.coalesce(F.col("c.customer_key"), F.lit(-1)).alias("customer_key"),
        F.to_date(F.col("o.occurred_at")).alias("order_date"), # Not exact order_date_key format yet, will format later if needed
        F.col("o.batch_paid_date").alias("paid_date"),
        
        # Status key: 1 PENDING, 2 PAID, 4 CANCELLED
        F.when(F.col("o.batch_has_cancelled") == 1, F.lit(4))
         .when(F.col("o.batch_has_paid") == 1, F.lit(2))
         .otherwise(F.lit(1)).alias("status_key"),
         
        F.col("o.data.subtotalAmount").alias("subtotal_amount"),
        F.col("o.data.taxAmount").alias("tax_amount"),
        F.col("o.data.shippingAmount").alias("shipping_amount"),
        F.col("o.data.totalAmount").alias("total_amount"),
        
        # GMV only if paid
        F.when(F.col("o.batch_has_paid") == 1, F.col("o.data.totalAmount")).otherwise(F.lit(0.0)).alias("gmv_amount"),
        F.when(F.col("o.batch_has_paid") == 1, F.lit(True)).otherwise(F.lit(False)).alias("is_paid"),
        
        F.col("o.occurred_at").alias("updated_at"),
        F.col("o.batch_cancelled_at").alias("cancelled_at")
    )
    return df

def build_fact_order_items(silver_df: DataFrame, dim_products: DataFrame) -> DataFrame:
    """
    Builds fact_order_items.
    One row per line item. Price snapshot preserved.
    """
    # We only care about items at ORDER_CREATED to get the snapshot
    orders = silver_df.filter(F.col("event_type") == "ORDER_CREATED")
    parsed = orders.withColumn("data", F.from_json(F.col("payload"), ORDER_PAYLOAD_SCHEMA))
    
    # Explode items
    exploded = parsed.withColumn("item", F.explode("data.items"))
    
    # Resolve product key
    with_prod = exploded.alias("o").join(
        dim_products.alias("p"),
        F.col("o.item.productId") == F.col("p.product_id"),
        "left"
    )
    
    df = with_prod.select(
        generate_surrogate_key("o.item.orderItemId").alias("order_item_key"),
        generate_surrogate_key("o.data.orderId").alias("order_key"),
        F.coalesce(F.col("p.product_key"), F.lit(-1)).alias("product_key"),
        F.to_date(F.col("o.occurred_at")).alias("order_date"),
        F.col("o.item.productSku").alias("product_sku_snapshot"),
        F.col("o.item.productTitle").alias("product_title_snapshot"),
        F.col("o.item.unitPrice").alias("unit_price_snapshot"),
        F.col("o.item.quantity").alias("quantity"),
        (F.col("o.item.unitPrice") * F.col("o.item.quantity")).alias("line_total_amount")
    )
    
    return df

def build_fact_payments(silver_df: DataFrame, dim_customers: DataFrame) -> DataFrame:
    """
    Builds fact_payments.
    One row per attempt. Audit only, NO GMV impact.
    """
    payments = silver_df.filter(F.col("aggregate_type") == "PAYMENT")
    parsed = payments.withColumn("data", F.from_json(F.col("payload"), PAYMENT_PAYLOAD_SCHEMA))
    
    with_cust = parsed.alias("p").join(
        dim_customers.alias("c"),
        F.col("p.data.customerId") == F.col("c.customer_id"),
        "left"
    )
    
    df = with_cust.select(
        generate_surrogate_key("p.data.paymentId").alias("payment_key"),
        F.col("p.data.paymentId").alias("payment_id"),
        generate_surrogate_key("p.data.orderId").alias("order_key"),
        F.coalesce(F.col("c.customer_key"), F.lit(-1)).alias("customer_key"),
        F.to_date(F.col("p.occurred_at")).alias("payment_date"),
        F.col("p.data.attemptNumber").alias("attempt_number"),
        F.col("p.data.status").alias("status"),
        F.col("p.data.amount").alias("payment_amount"),
        F.col("p.data.currency").alias("currency"),
        F.col("p.data.paymentMethod").alias("payment_method"),
        F.col("p.data.errorCode").alias("error_code"),
        F.col("p.occurred_at").alias("created_at")
    )
    
    return df

def build_fact_product_views(silver_df: DataFrame, dim_customers: DataFrame, dim_products: DataFrame) -> DataFrame:
    """
    Builds fact_product_views.
    One row per view event.
    """
    views = silver_df.filter(F.col("event_type") == "PRODUCT_VIEWED")
    parsed = views.withColumn("data", F.from_json(F.col("payload"), VIEW_PAYLOAD_SCHEMA))
    
    with_cust = parsed.alias("v").join(
        dim_customers.alias("c"),
        F.col("v.data.customerId") == F.col("c.customer_id"),
        "left"
    )
    
    with_prod = with_cust.join(
        dim_products.alias("p"),
        F.col("v.data.productId") == F.col("p.product_id"),
        "left"
    )
    
    df = with_prod.select(
        generate_surrogate_key("v.event_id").alias("view_key"),
        F.col("v.event_id").alias("event_id"),
        F.coalesce(F.col("c.customer_key"), F.lit(-1)).alias("customer_key"),
        F.coalesce(F.col("p.product_key"), F.lit(-1)).alias("product_key"),
        F.to_date(F.col("v.occurred_at")).alias("view_date"),
        F.col("v.data.sessionToken").alias("session_token"),
        F.col("v.occurred_at").alias("created_at")
    )
    
    return df
