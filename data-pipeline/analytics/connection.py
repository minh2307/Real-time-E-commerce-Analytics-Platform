import duckdb
from contextlib import contextmanager
from typing import Generator
import os

from analytics.config import AnalyticsConfig

@contextmanager
def get_duckdb_connection(config: AnalyticsConfig) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    if config.duckdb_path != ":memory:":
        dirname = os.path.dirname(config.duckdb_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
            
    conn = duckdb.connect(database=config.duckdb_path)
    try:
        yield conn
    finally:
        conn.close()
