import duckdb
import os

def run_query(conn: duckdb.DuckDBPyConnection, query_name: str) -> duckdb.DuckDBPyRelation:
    sql_path = os.path.join(os.path.dirname(__file__), "sql", f"{query_name}.sql")
    with open(sql_path, "r") as f:
        sql = f.read()
    return conn.sql(sql)

def get_daily_gmv(conn: duckdb.DuckDBPyConnection):
    return run_query(conn, "daily_gmv")

def get_product_revenue(conn: duckdb.DuckDBPyConnection):
    return run_query(conn, "product_revenue")

def get_category_revenue(conn: duckdb.DuckDBPyConnection):
    return run_query(conn, "category_revenue")

def get_payment_analytics(conn: duckdb.DuckDBPyConnection):
    return run_query(conn, "payment_analytics")

def get_product_views(conn: duckdb.DuckDBPyConnection):
    return run_query(conn, "product_views")

def get_customer_analytics(conn: duckdb.DuckDBPyConnection):
    return run_query(conn, "customer_analytics")

def get_kpis(conn: duckdb.DuckDBPyConnection):
    return run_query(conn, "kpis")
