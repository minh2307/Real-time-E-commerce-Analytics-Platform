import psycopg2
from confluent_kafka import Producer
import json
import uuid
import time
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def main():
    logger.info("Generating E2E seed data in PostgreSQL...")
    conn = psycopg2.connect(
        dbname="ecommerce_db",
        user="ecommerce",
        password="ecommerce_password",
        host="localhost",
        port=15432
    )
    conn.autocommit = True
    cursor = conn.cursor()

    order_id = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    product_id = str(uuid.uuid4())

    events = [
        {
            "event_type": "USER_REGISTERED",
            "aggregate_type": "USER",
            "aggregate_id": customer_id,
            "payload": {
                "user_id": customer_id,
                "email": "e2e@example.com",
                "username": "e2e_user",
                "full_name": "E2E User",
                "role": "USER",
                "status": "ACTIVE"
            }
        },
        {
            "event_type": "PRODUCT_CREATED",
            "aggregate_type": "PRODUCT",
            "aggregate_id": product_id,
            "payload": {
                "product_id": product_id,
                "sku": "E2E-PROD-1",
                "title": "E2E Product",
                "description": "E2E Product Description",
                "category_id": "cat_1",
                "current_price": 100.0,
                "is_active": True
            }
        },
        {
            "event_type": "ORDER_CREATED",
            "aggregate_type": "ORDER",
            "aggregate_id": order_id,
            "payload": {
                "order_id": order_id,
                "customer_id": customer_id,
                "total_amount": 100.0,
                "subtotal_amount": 100.0,
                "tax_amount": 0.0,
                "shipping_amount": 0.0,
                "items": [
                    {
                        "order_item_id": str(uuid.uuid4()),
                        "product_id": product_id,
                        "quantity": 1,
                        "unit_price": 100.0,
                        "product_sku": "E2E-PROD-1",
                        "product_title": "E2E Product"
                    }
                ]
            }
        },
        {
            "event_type": "ORDER_PAID",
            "aggregate_type": "ORDER",
            "aggregate_id": order_id,
            "payload": {
                "order_id": order_id,
                "customer_id": customer_id,
                "total_amount": 100.0,
                "subtotal_amount": 100.0,
                "tax_amount": 0.0,
                "shipping_amount": 0.0,
                "items": []
            }
        }
    ]

    event_ids = []
    for evt in events:
        event_id = str(uuid.uuid4())
        event_ids.append(event_id)
        cursor.execute(
            """
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status)
            VALUES (%s, %s, %s, %s, %s, 'PENDING')
            """,
            (event_id, evt["aggregate_type"], evt["aggregate_id"], evt["event_type"], json.dumps(evt["payload"]))
        )

    logger.info(f"Inserted {len(events)} events into outbox_events.")

    # Now mimic OutboxPoller to send to Kafka, so we don't depend on the Java backend running
    logger.info("Mimicking OutboxPoller to publish to Kafka...")
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    for event_id in event_ids:
        cursor.execute("SELECT aggregate_type, aggregate_id, event_type, payload, created_at FROM outbox_events WHERE id = %s", (event_id,))
        row = cursor.fetchone()

        kafka_payload = {
            "event_id": event_id,
            "aggregate_type": row[0],
            "aggregate_id": str(row[1]),
            "event_type": row[2],
            "payload": row[3],
            "occurred_at": row[4].isoformat()
        }

        producer.produce('ecommerce.events', key=str(row[1]), value=json.dumps(kafka_payload))
        cursor.execute("UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = %s", (event_id,))

    producer.flush()
    conn.close()
    logger.info("Successfully published E2E seed data to Kafka.")

if __name__ == "__main__":
    main()
