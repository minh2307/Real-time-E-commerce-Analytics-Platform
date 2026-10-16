import pytest
import os
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from tempfile import TemporaryDirectory
from analytics.config import AnalyticsConfig
from analytics.connection import get_duckdb_connection
from analytics.views import register_gold_views
from analytics import queries

@pytest.fixture
def temp_gold_dir():
    with TemporaryDirectory() as d:
        yield d

@pytest.fixture
def duckdb_conn(temp_gold_dir):
    config = AnalyticsConfig(gold_output_dir=temp_gold_dir, duckdb_path=":memory:")
    with get_duckdb_connection(config) as conn:
        yield conn, config

def write_parquet(path, table):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)

def create_mock_data(gold_dir):
    # dim_customers
    customers = pa.table({
        "customer_key": [1, -1],
        "customer_id": ["cust1", "-1"],
        "email": ["cust1@example.com", "Unknown"],
        "username": ["c1", "unk"],
        "full_name": ["C One", "Unknown"],
        "role": ["USER", "NONE"],
        "status": ["ACTIVE", "NONE"],
        "registration_date": [None, None]
    })
    write_parquet(os.path.join(gold_dir, "dimensions/dim_customers", "part1.parquet"), customers)
    
    # dim_products
    products = pa.table({
        "product_key": [1, 2],
        "product_id": ["prod1", "prod2"],
        "sku": ["SKU1", "SKU2"],
        "title": ["Product 1", "Product 2"],
        "description": ["D1", "D2"],
        "category_key": [1, 1],
        "category_id": ["cat1", "cat1"],
        "current_price": [150.0, 250.0],
        "is_active": [True, True]
    })
    write_parquet(os.path.join(gold_dir, "dimensions/dim_products", "part1.parquet"), products)

    # dim_categories
    categories = pa.table({
        "category_key": [1],
        "category_id": ["cat1"],
        "name": ["Category 1"],
        "slug": ["cat-1"],
        "parent_category_id": [None],
        "description": ["Desc"]
    })
    write_parquet(os.path.join(gold_dir, "dimensions/dim_categories", "part1.parquet"), categories)

    # fact_orders
    # Order 1: Paid (GMV 100.0) - cust1
    # Order 2: Pending (GMV 0.0) - unknown cust
    orders = pa.table({
        "order_key": [101, 102],
        "order_id": ["ord1", "ord2"],
        "customer_key": [1, -1],
        "order_date": ["2023-01-01", "2023-01-01"],
        "paid_date": ["2023-01-01", None],
        "status_key": [2, 1],
        "subtotal_amount": [100.0, 50.0],
        "tax_amount": [0.0, 0.0],
        "shipping_amount": [0.0, 0.0],
        "total_amount": [100.0, 50.0],
        "gmv_amount": [100.0, 0.0],
        "is_paid": [True, False],
        "updated_at": [None, None],
        "cancelled_at": [None, None]
    })
    write_parquet(os.path.join(gold_dir, "facts/fact_orders", "part1.parquet"), orders)

    # fact_order_items
    # Order 1 has 2 items (P1 x1 @ 40, P2 x1 @ 60 = 100)
    # Note historical snapshot: P1 price was 40, even though current_price is 150
    items = pa.table({
        "order_item_key": [1001, 1002],
        "order_key": [101, 101],
        "product_key": [1, 2],
        "order_date": ["2023-01-01", "2023-01-01"],
        "product_sku_snapshot": ["SKU1", "SKU2"],
        "product_title_snapshot": ["Product 1", "Product 2"],
        "unit_price_snapshot": [40.0, 60.0],
        "quantity": [1, 1],
        "line_total_amount": [40.0, 60.0]
    })
    write_parquet(os.path.join(gold_dir, "facts/fact_order_items", "part1.parquet"), items)

    # fact_payments
    # Order 1 had 3 payments attempts: FAIL, FAIL, SUCCESS
    payments = pa.table({
        "payment_key": [501, 502, 503],
        "payment_id": ["pay1", "pay2", "pay3"],
        "order_key": [101, 101, 101],
        "customer_key": [1, 1, 1],
        "payment_date": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "attempt_number": [1, 2, 3],
        "status": ["FAILED", "FAILED", "COMPLETED"],
        "payment_amount": [100.0, 100.0, 100.0],
        "currency": ["USD", "USD", "USD"],
        "payment_method": ["CC", "CC", "CC"],
        "error_code": ["E1", "E2", None],
        "created_at": [None, None, None]
    })
    write_parquet(os.path.join(gold_dir, "facts/fact_payments", "part1.parquet"), payments)

    # fact_product_views
    views = pa.table({
        "view_key": [901, 902],
        "event_id": ["evt1", "evt2"],
        "customer_key": [-1, 1],
        "product_key": [1, 1],
        "view_date": ["2023-01-01", "2023-01-01"],
        "session_token": ["sess1", "sess1"],
        "created_at": [None, None]
    })
    write_parquet(os.path.join(gold_dir, "facts/fact_product_views", "part1.parquet"), views)

def test_daily_gmv(duckdb_conn):
    conn, config = duckdb_conn
    create_mock_data(config.gold_output_dir)
    register_gold_views(conn, config)
    
    res = queries.get_daily_gmv(conn).fetchall()
    assert len(res) == 1
    # row: date, orders_count, paid_orders_count, gmv
    assert res[0][1] == 2 # orders_count
    assert res[0][2] == 1 # paid_orders_count
    assert res[0][3] == 100.0 # GMV only from Paid
    
    # Idempotency
    res2 = queries.get_daily_gmv(conn).fetchall()
    assert res == res2

def test_product_revenue(duckdb_conn):
    conn, config = duckdb_conn
    create_mock_data(config.gold_output_dir)
    register_gold_views(conn, config)
    
    res = queries.get_product_revenue(conn).fetchall()
    assert len(res) == 2
    # P2: revenue 60
    assert res[0][4] == 60.0
    # P1: revenue 40 (unit_price_snapshot 40 * qty 1)
    assert res[1][4] == 40.0
    
def test_category_revenue(duckdb_conn):
    conn, config = duckdb_conn
    create_mock_data(config.gold_output_dir)
    register_gold_views(conn, config)
    
    res = queries.get_category_revenue(conn).fetchall()
    assert len(res) == 1
    # category 1, orders=1, units=2, rev=100
    assert res[0][2] == 1
    assert res[0][4] == 100.0

def test_payment_analytics(duckdb_conn):
    conn, config = duckdb_conn
    create_mock_data(config.gold_output_dir)
    register_gold_views(conn, config)
    
    res = queries.get_payment_analytics(conn).fetchall()
    assert len(res) == 2 # COMPLETED and FAILED
    failed = [r for r in res if r[1] == 'FAILED'][0]
    completed = [r for r in res if r[1] == 'COMPLETED'][0]
    
    assert failed[2] == 2 # 2 attempts
    assert completed[2] == 1 # 1 attempt

def test_product_views(duckdb_conn):
    conn, config = duckdb_conn
    create_mock_data(config.gold_output_dir)
    register_gold_views(conn, config)
    
    res = queries.get_product_views(conn).fetchall()
    assert len(res) == 1
    assert res[0][0] == 'Product 1'
    assert res[0][1] == 2 # 2 views
    assert res[0][2] == 1 # 1 unique session

def test_customer_analytics_and_unknown_members(duckdb_conn):
    conn, config = duckdb_conn
    create_mock_data(config.gold_output_dir)
    register_gold_views(conn, config)
    
    res = queries.get_customer_analytics(conn).fetchall()
    assert len(res) == 2
    # Check that unknown customer wasn't dropped
    emails = [r[0] for r in res]
    assert 'Unknown' in emails
    assert 'cust1@example.com' in emails

def test_kpis_no_multiplication(duckdb_conn):
    conn, config = duckdb_conn
    create_mock_data(config.gold_output_dir)
    register_gold_views(conn, config)
    
    res = queries.get_kpis(conn).fetchall()
    assert len(res) == 1
    # row: total_orders, paid_orders, gmv, total_items_sold, total_product_views
    row = res[0]
    assert row[0] == 2 # total_orders
    assert row[1] == 1 # paid_orders
    assert row[2] == 100.0 # gmv (despite 3 payment attempts, GMV is 100.0)
    assert row[3] == 2 # items sold
    assert row[4] == 2 # product views
