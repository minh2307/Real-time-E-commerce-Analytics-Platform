import os
import duckdb
from analytics.config import AnalyticsConfig

def register_gold_views(conn: duckdb.DuckDBPyConnection, config: AnalyticsConfig):
    datasets = {
        "dim_customers": "dimensions",
        "dim_categories": "dimensions",
        "dim_products": "dimensions",
        "fact_orders": "facts",
        "fact_order_items": "facts",
        "fact_payments": "facts",
        "fact_product_views": "facts"
    }
    
    for dataset, parent in datasets.items():
        dataset_dir = os.path.join(config.gold_output_dir, parent, dataset)
        path = os.path.join(dataset_dir, "*.parquet")
        os.makedirs(dataset_dir, exist_ok=True)
        
        has_files = any(f.endswith(".parquet") for f in os.listdir(dataset_dir)) if os.path.exists(dataset_dir) else False
        
        if has_files:
            conn.execute(f"CREATE OR REPLACE VIEW {dataset} AS SELECT * FROM read_parquet('{path}')")
        else:
            pass
