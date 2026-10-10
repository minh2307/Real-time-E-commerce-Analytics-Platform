import requests
import psycopg2
import uuid
import time
import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = "http://localhost:8080"

def main():
    email = f"e2e_{uuid.uuid4()}@example.com"
    password = "password123"

    # 1. Register User
    logger.info(f"Registering user: {email}")
    resp = requests.post(f"{API_URL}/api/auth/register", json={
        "email": email,
        "password": password,
        "firstName": "E2E",
        "lastName": "User"
    })
    resp.raise_for_status()

    # 2. Promote to ADMIN via DB

    # 3. Login
    logger.info("Logging in...")
    resp = requests.post(f"{API_URL}/api/auth/login", json={
        "email": email,
        "password": password
    })
    resp.raise_for_status()
    token = resp.json()["token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 4. Create Category
    logger.info("Creating Category...")
    cat_resp = requests.post(f"{API_URL}/api/v1/admin/categories", headers=headers, json={
        "name": "E2E Category",
        "slug": f"e2e-category-{uuid.uuid4().hex[:8]}",
        "description": "Category for E2E tests"
    })
    cat_resp.raise_for_status()
    category_id = cat_resp.json()["id"]

    # 5. Create Product
    logger.info("Creating Product...")
    prod_resp = requests.post(f"{API_URL}/api/v1/admin/products", headers=headers, json={
        "categoryId": category_id,
        "sku": f"E2E-PROD-{uuid.uuid4().hex[:8]}",
        "title": "E2E Product",
        "description": "Product for E2E tests",
        "price": 100.0
    })
    prod_resp.raise_for_status()
    product_id = prod_resp.json()["id"]

    # 5.2. Restock Inventory
    logger.info("Restocking Inventory...")
    inv_resp = requests.put(f"{API_URL}/api/v1/admin/inventory/{product_id}", headers=headers, json={
        "additionalQuantity": 10
    })
    if inv_resp.status_code != 200: print(inv_resp.text)
    inv_resp.raise_for_status()

    # 5.5. Add to Cart
    logger.info("Adding to Cart...")
    cart_resp = requests.post(f"{API_URL}/api/v1/cart/items", headers=headers, json={
        "productId": product_id,
        "quantity": 2
    })
    if cart_resp.status_code != 200: print(cart_resp.text)
    cart_resp.raise_for_status()

    # 6. Create Order
    logger.info("Creating Order...")
    order_idempotency = str(uuid.uuid4())
    order_resp = requests.post(f"{API_URL}/api/v1/orders", headers={**headers, "Idempotency-Key": order_idempotency}, json={
        "shippingAddress": "123 E2E St",
        "billingAddress": "123 E2E St"
    })
    print("Order Response:", order_resp.status_code, order_resp.text)
    order_resp.raise_for_status()
    order_id = order_resp.json()["id"]
    total_amount = order_resp.json()["totalAmount"]

    # 7. Pay for Order
    logger.info("Paying for Order...")
    payment_idempotency = str(uuid.uuid4())
    pay_resp = requests.post(f"{API_URL}/api/payments", headers={**headers, "Idempotency-Key": payment_idempotency}, json={
        "orderId": order_id,
        "amount": total_amount,
        "currency": "USD",
        "paymentMethod": "CREDIT_CARD"
    })
    pay_resp.raise_for_status()

    logger.info("Successfully generated app traffic!")

if __name__ == "__main__":
    main()
