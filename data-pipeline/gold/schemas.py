from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
)

# We define the JSON schemas that will be used to parse the 'payload' column from Silver events

ORDER_ITEM_SCHEMA = StructType([
    StructField("orderItemId", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unitPrice", DoubleType(), True),
    StructField("productSku", StringType(), True),
    StructField("productTitle", StringType(), True)
])

ORDER_PAYLOAD_SCHEMA = StructType([
    StructField("orderId", StringType(), True),
    StructField("customerId", StringType(), True),
    StructField("totalAmount", DoubleType(), True),
    StructField("subtotalAmount", DoubleType(), True),
    StructField("taxAmount", DoubleType(), True),
    StructField("shippingAmount", DoubleType(), True),
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA), True)
])

PAYMENT_PAYLOAD_SCHEMA = StructType([
    StructField("paymentId", StringType(), True),
    StructField("orderId", StringType(), True),
    StructField("customerId", StringType(), True),
    StructField("attemptNumber", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("errorCode", StringType(), True)
])

USER_PAYLOAD_SCHEMA = StructType([
    StructField("userId", StringType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("fullName", StringType(), True),
    StructField("role", StringType(), True),
    StructField("status", StringType(), True)
])

PRODUCT_PAYLOAD_SCHEMA = StructType([
    StructField("productId", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("categoryId", StringType(), True),
    StructField("currentPrice", DoubleType(), True),
    StructField("isActive", BooleanType(), True)
])

CATEGORY_PAYLOAD_SCHEMA = StructType([
    StructField("categoryId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("parentCategoryId", StringType(), True),
    StructField("description", StringType(), True)
])

VIEW_PAYLOAD_SCHEMA = StructType([
    StructField("productId", StringType(), True),
    StructField("sessionToken", StringType(), True),
    StructField("customerId", StringType(), True)
])
