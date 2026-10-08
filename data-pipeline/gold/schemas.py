from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
)

# We define the JSON schemas that will be used to parse the 'payload' column from Silver events

ORDER_ITEM_SCHEMA = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("product_sku", StringType(), True),
    StructField("product_title", StringType(), True)
])

ORDER_PAYLOAD_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("subtotal_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("shipping_amount", DoubleType(), True),
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA), True)
])

PAYMENT_PAYLOAD_SCHEMA = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("attempt_number", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("error_code", StringType(), True)
])

USER_PAYLOAD_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("role", StringType(), True),
    StructField("status", StringType(), True)
])

PRODUCT_PAYLOAD_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("is_active", BooleanType(), True)
])

CATEGORY_PAYLOAD_SCHEMA = StructType([
    StructField("category_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("parent_category_id", StringType(), True),
    StructField("description", StringType(), True)
])

VIEW_PAYLOAD_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("session_token", StringType(), True),
    StructField("customer_id", StringType(), True)
])
