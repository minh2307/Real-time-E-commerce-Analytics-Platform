import pytest
import os
import subprocess
import sys
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from tempfile import TemporaryDirectory

def write_parquet(path, table):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)

def run_validator(data_dir):
    env = os.environ.copy()
    env["GOLD_OUTPUT_DIR"] = os.path.join(data_dir, "gold")
    env["PYTHONPATH"] = os.path.abspath("data-pipeline")
    script_path = os.path.abspath("validate_analytics.py")
    return subprocess.run(
        [sys.executable, script_path], 
        env=env, 
        capture_output=True, 
        text=True
    )

def create_valid_gold(data_dir):
    gold_dir = os.path.join(data_dir, "gold")
    # dimensions
    write_parquet(os.path.join(gold_dir, "dimensions/dim_customers/part1.parquet"), pa.table({
        "customer_key": [1, -1], "customer_id": ["c1", "-1"], "email": ["e2e_test@example.com", "Unknown"],
        "username": ["c1", "u"], "full_name": ["C1", "U"], "role": ["USER", "NONE"], "status": ["ACTIVE", "NONE"], "registration_date": [None, None]
    }))
    write_parquet(os.path.join(gold_dir, "dimensions/dim_products/part1.parquet"), pa.table({
        "product_key": [1], "product_id": ["p1"], "sku": ["S1"], "title": ["P1"], "description": ["D1"],
        "category_key": [1], "category_id": ["cat1"], "current_price": [10.0], "is_active": [True]
    }))
    write_parquet(os.path.join(gold_dir, "dimensions/dim_categories/part1.parquet"), pa.table({
        "category_key": [1], "category_id": ["cat1"], "name": ["Cat1"], "slug": ["cat1"], "parent_category_id": [None], "description": [""]
    }))
    
    # facts
    write_parquet(os.path.join(gold_dir, "facts/fact_orders/part1.parquet"), pa.table({
        "order_key": [1], "order_id": ["o1"], "customer_key": [1], "order_date": ["2023-01-01"], "paid_date": ["2023-01-01"],
        "status_key": [2], "subtotal_amount": [10.0], "tax_amount": [0.0], "shipping_amount": [0.0], "total_amount": [10.0],
        "gmv_amount": [10.0], "is_paid": [True], "updated_at": [None], "cancelled_at": [None]
    }))
    write_parquet(os.path.join(gold_dir, "facts/fact_order_items/part1.parquet"), pa.table({
        "order_item_key": [1], "order_key": [1], "product_key": [1], "order_date": ["2023-01-01"],
        "product_sku_snapshot": ["S1"], "product_title_snapshot": ["P1"], "unit_price_snapshot": [10.0], "quantity": [1], "line_total_amount": [10.0]
    }))
    write_parquet(os.path.join(gold_dir, "facts/fact_payments/part1.parquet"), pa.table({
        "payment_key": [1, 2], "payment_id": ["pay1", "pay2"], "order_key": [1, 1], "customer_key": [1, 1], "payment_date": ["2023-01-01", "2023-01-01"],
        "attempt_number": [1, 2], "status": ["FAILED", "COMPLETED"], "payment_amount": [10.0, 10.0], "currency": ["USD", "USD"],
        "payment_method": ["CC", "CC"], "error_code": ["E1", None], "created_at": [None, None]
    }))
    write_parquet(os.path.join(gold_dir, "facts/fact_product_views/part1.parquet"), pa.table({
        "view_key": [1], "event_id": ["e1"], "customer_key": [1], "product_key": [1], "view_date": ["2023-01-01"], "session_token": ["s1"], "created_at": [None]
    }))

def test_valid_dataset():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        res = run_validator(d)
        assert res.returncode == 0
        assert "VALIDATION SUCCESSFUL" in res.stdout
        assert "Order Grain (Exactly 1 row per order in fact_orders): PASS" in res.stdout

def test_missing_dataset():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        shutil.rmtree(os.path.join(d, "gold/facts/fact_payments"))
        res = run_validator(d)
        assert res.returncode == 1
        assert "FAIL (No data)" in res.stdout

def test_no_parquet_files():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        os.remove(os.path.join(d, "gold/facts/fact_orders/part1.parquet"))
        with open(os.path.join(d, "gold/facts/fact_orders/_SUCCESS"), "w") as f:
            f.write("")
        res = run_validator(d)
        assert res.returncode == 1
        assert "fact_orders: FAIL (No data)" in res.stdout

def test_analytical_query_exception():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        # break schema of fact_orders
        write_parquet(os.path.join(d, "gold/facts/fact_orders/part1.parquet"), pa.table({"bad_col": [1]}))
        res = run_validator(d)
        assert res.returncode == 1
        assert "FAIL" in res.stdout
        assert "Integration Failed" in res.stdout or "VALIDATION FAILED" in res.stdout

def test_gmv_violation():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        write_parquet(os.path.join(d, "gold/facts/fact_orders/part1.parquet"), pa.table({
            "order_key": [1], "order_id": ["o1"], "customer_key": [1], "order_date": ["2023-01-01"], "paid_date": ["2023-01-01"],
            "status_key": [2], "subtotal_amount": [10.0], "tax_amount": [0.0], "shipping_amount": [0.0], "total_amount": [10.0],
            "gmv_amount": [0.0], "is_paid": [True], "updated_at": [None], "cancelled_at": [None] # GMV 0 but paid
        }))
        res = run_validator(d)
        assert res.returncode == 1
        assert "GMV rule: FAIL" in res.stdout

def test_duplicate_order_fact():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        write_parquet(os.path.join(d, "gold/facts/fact_orders/part1.parquet"), pa.table({
            "order_key": [1, 2], "order_id": ["o1", "o1"], "customer_key": [1, 1], "order_date": ["2023-01-01", "2023-01-01"], "paid_date": ["2023-01-01", "2023-01-01"],
            "status_key": [2, 2], "subtotal_amount": [10.0, 10.0], "tax_amount": [0.0, 0.0], "shipping_amount": [0.0, 0.0], "total_amount": [10.0, 10.0],
            "gmv_amount": [10.0, 10.0], "is_paid": [True, True], "updated_at": [None, None], "cancelled_at": [None, None]
        }))
        res = run_validator(d)
        assert res.returncode == 1
        assert "Order Grain: FAIL" in res.stdout

def test_join_payment_gmv_multiplication():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        # In create_valid_gold, order_key 1 is paid, gmv_amount=10.0, and has 2 payments: FAILED (10.0) and COMPLETED (10.0).
        # This is valid. Let's make it invalid by having a COMPLETED payment of 5.0 (mismatch)
        write_parquet(os.path.join(d, "gold/facts/fact_payments/part1.parquet"), pa.table({
            "payment_key": [1, 2], "payment_id": ["pay1", "pay2"], "order_key": [1, 1], "customer_key": [1, 1], "payment_date": ["2023-01-01", "2023-01-01"],
            "attempt_number": [1, 2], "status": ["FAILED", "COMPLETED"], "payment_amount": [10.0, 5.0], "currency": ["USD", "USD"],
            "payment_method": ["CC", "CC"], "error_code": ["E1", None], "created_at": [None, None]
        }))
        res = run_validator(d)
        assert res.returncode == 1
        assert "JOIN Order & Payment: FAIL" in res.stdout

def test_fact_multiplication_items():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        write_parquet(os.path.join(d, "gold/facts/fact_order_items/part1.parquet"), pa.table({
            "order_item_key": [1], "order_key": [1], "product_key": [1], "order_date": ["2023-01-01"],
            "product_sku_snapshot": ["S1"], "product_title_snapshot": ["P1"], "unit_price_snapshot": [10.0], "quantity": [1], "line_total_amount": [5.0] # 5 != 10
        }))
        res = run_validator(d)
        assert res.returncode == 1
        assert "Fact multiplication: FAIL" in res.stdout

def test_unit_price_snapshot():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        write_parquet(os.path.join(d, "gold/facts/fact_order_items/part1.parquet"), pa.table({
            "order_item_key": [1], "order_key": [1], "product_key": [1], "order_date": ["2023-01-01"],
            "product_sku_snapshot": ["S1"], "product_title_snapshot": ["P1"], "unit_price_snapshot": [0.0], "quantity": [1], "line_total_amount": [10.0]
        }))
        res = run_validator(d)
        assert res.returncode == 1
        assert "Unit price snapshot: FAIL" in res.stdout

def test_missing_e2e_data():
    with TemporaryDirectory() as d:
        create_valid_gold(d)
        write_parquet(os.path.join(d, "gold/dimensions/dim_customers/part1.parquet"), pa.table({
            "customer_key": [1], "customer_id": ["c1"], "email": ["normal@example.com"],
            "username": ["c1"], "full_name": ["C1"], "role": ["USER"], "status": ["ACTIVE"], "registration_date": [None]
        }))
        res = run_validator(d)
        assert res.returncode == 1
        assert "E2E Real App Traffic Flow: FAIL" in res.stdout
