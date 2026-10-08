import os
import duckdb
from analytics.config import AnalyticsConfig

def register_gold_views(conn: duckdb.DuckDBPyConnection, config: AnalyticsConfig):
    """
    Registers Gold Parquet datasets as views in DuckDB.
    """
    datasets = [
        "dim_customers",
        "dim_categories",
        "dim_products",
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "fact_product_views"
    ]
    
    for dataset in datasets:
        path = os.path.join(config.gold_output_dir, dataset, "*.parquet")
        # Ensure directory exists so DuckDB glob doesn't complain about missing directory.
        # But if there are no parquet files, read_parquet might still complain.
        dataset_dir = os.path.join(config.gold_output_dir, dataset)
        os.makedirs(dataset_dir, exist_ok=True)
        
        # Check if there is at least one parquet file
        has_files = any(f.endswith(".parquet") for f in os.listdir(dataset_dir)) if os.path.exists(dataset_dir) else False
        
        if has_files:
            conn.execute(f"CREATE OR REPLACE VIEW {dataset} AS SELECT * FROM read_parquet('{path}')")
        else:
            # We cannot create a view from an empty glob without knowing the schema.
            # In a real environment, Gold pipeline should be run first.
            pass
