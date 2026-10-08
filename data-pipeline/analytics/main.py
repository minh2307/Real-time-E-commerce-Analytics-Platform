from analytics.config import load_config
from analytics.connection import get_duckdb_connection
from analytics.views import register_gold_views
from analytics import queries

def main():
    config = load_config()
    with get_duckdb_connection(config) as conn:
        register_gold_views(conn, config)
        
        print("KPIs:")
        kpis = queries.get_kpis(conn)
        kpis.show()

if __name__ == "__main__":
    main()
