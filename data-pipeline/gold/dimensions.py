from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType
from gold.schemas import USER_PAYLOAD_SCHEMA, PRODUCT_PAYLOAD_SCHEMA, CATEGORY_PAYLOAD_SCHEMA

def generate_surrogate_key(col_name: str) -> F.Column:
    """
    Generates a deterministic surrogate key using xxhash64.
    """
    return F.abs(F.xxhash64(F.col(col_name)))

def build_dim_customers(silver_df: DataFrame) -> DataFrame:
    """
    Builds dim_customers from USER_REGISTERED events.
    Applies SCD Type 1 by taking the latest event per customer_id.
    Adds the Unknown Member (-1) implicitly via a union or ensures downstream lookup handles it.
    """
    users = silver_df.filter(F.col("event_type") == "USER_REGISTERED")
    parsed = users.withColumn("data", F.from_json(F.col("payload"), USER_PAYLOAD_SCHEMA))
    
    # SCD Type 1: latest record overwrites
    window = F.expr("row_number() OVER (PARTITION BY data.userId ORDER BY occurred_at DESC)")
    latest = parsed.withColumn("rn", window).filter(F.col("rn") == 1)
    
    dim = latest.select(
        generate_surrogate_key("data.userId").alias("customer_key"),
        F.col("data.userId").alias("customer_id"),
        F.col("data.email").alias("email"),
        F.col("data.username").alias("username"),
        F.col("data.fullName").alias("full_name"),
        F.col("data.role").alias("role"),
        F.col("data.status").alias("status"),
        F.to_date(F.col("occurred_at")).alias("registration_date") # Simplified from date_key for now
    )
    
    return dim

def build_dim_categories(silver_df: DataFrame) -> DataFrame:
    """
    Builds dim_categories.
    """
    cats = silver_df.filter(F.col("aggregate_type") == "CATEGORY")
    parsed = cats.withColumn("data", F.from_json(F.col("payload"), CATEGORY_PAYLOAD_SCHEMA))
    
    window = F.expr("row_number() OVER (PARTITION BY data.categoryId ORDER BY occurred_at DESC)")
    latest = parsed.withColumn("rn", window).filter(F.col("rn") == 1)
    
    dim = latest.select(
        generate_surrogate_key("data.categoryId").alias("category_key"),
        F.col("data.categoryId").alias("category_id"),
        F.col("data.name").alias("name"),
        F.col("data.slug").alias("slug"),
        F.col("data.parentCategoryId").alias("parent_category_id"),
        F.col("data.description").alias("description")
    )
    
    return dim

def build_dim_products(silver_df: DataFrame, dim_categories: DataFrame) -> DataFrame:
    """
    Builds dim_products.
    """
    prods = silver_df.filter(F.col("aggregate_type") == "PRODUCT")
    parsed = prods.withColumn("data", F.from_json(F.col("payload"), PRODUCT_PAYLOAD_SCHEMA))
    
    window = F.expr("row_number() OVER (PARTITION BY data.productId ORDER BY occurred_at DESC)")
    latest = parsed.withColumn("rn", window).filter(F.col("rn") == 1)
    
    # Resolve category_key
    with_cat = latest.alias("p").join(
        dim_categories.alias("c"),
        F.col("p.data.categoryId") == F.col("c.category_id"),
        "left"
    )
    
    dim = with_cat.select(
        generate_surrogate_key("p.data.productId").alias("product_key"),
        F.col("p.data.productId").alias("product_id"),
        F.col("p.data.sku").alias("sku"),
        F.col("p.data.title").alias("title"),
        F.col("p.data.description").alias("description"),
        F.coalesce(F.col("c.category_key"), F.lit(-1)).alias("category_key"),
        F.col("p.data.categoryId").alias("category_id"),
        F.col("p.data.currentPrice").alias("current_price"),
        F.col("p.data.isActive").alias("is_active")
    )
    
    return dim
