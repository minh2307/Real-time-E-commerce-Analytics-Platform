from datetime import datetime, timedelta
import os
import subprocess
import time

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# Helper to get the absolute path to the virtual environment python
VENV_PYTHON = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".venv-de", "bin", "python")
PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(PIPELINE_DIR)

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # "Avoid excessive retries that waste RAM"
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'ecommerce_local_e2e',
    default_args=default_args,
    description='E2E Local MVP Pipeline for Real-time E-commerce Analytics',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mvp', 'local', 'e2e'],
    max_active_runs=1
)

def _check_infrastructure():
    print("TASK START: Checking infrastructure")
    import socket

    # Check Postgres
    print("Checking PostgreSQL on localhost:15432...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(('localhost', 15432)) != 0:
            raise RuntimeError("PostgreSQL is not reachable on localhost:15432")

    # Check Kafka
    print("Checking Kafka on localhost:9092...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(('localhost', 9092)) != 0:
            raise RuntimeError("Kafka is not reachable on localhost:9092")

    # Check Spark/Python
    if not os.path.exists(VENV_PYTHON):
        raise RuntimeError(f"Python not found at {VENV_PYTHON}")

    print("TASK END")

def _resource_profiling():
    print("TASK START: Resource Profiling")
    data_dir = os.path.join(PIPELINE_DIR, "data")
    if os.path.exists(data_dir):
        # Calculate size
        total_size = 0
        for dirpath, _, filenames in os.walk(data_dir):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        print(f"Total data directory size: {total_size / (1024 * 1024):.2f} MB")
    print("TASK END")

t_check_infra = PythonOperator(
    task_id='check_infrastructure',
    python_callable=_check_infrastructure,
    dag=dag
)

t_produce_events = BashOperator(
    task_id='produce_events',
    bash_command=f"cd \"{PROJECT_ROOT}\" && export PYTHONPATH=\"{PROJECT_ROOT}/data-pipeline\" && \"{VENV_PYTHON}\" data-pipeline/scripts/generate_e2e_data.py",
    dag=dag
)

t_bronze_ingestion = BashOperator(
    task_id='bronze_ingestion',
    bash_command=f"cd \"{PROJECT_ROOT}\" && export PYTHONPATH=\"{PROJECT_ROOT}/data-pipeline\" && \"{VENV_PYTHON}\" -m bronze.run_batch",
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

t_silver_transformation = BashOperator(
    task_id='silver_transformation',
    bash_command=f"cd \"{PROJECT_ROOT}\" && export PYTHONPATH=\"{PROJECT_ROOT}/data-pipeline\" && export PYSPARK_PYTHON=\"{VENV_PYTHON}\" && export PYSPARK_DRIVER_PYTHON=\"{VENV_PYTHON}\" && \"{VENV_PYTHON}\" -m silver.main",
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

t_gold_transformation = BashOperator(
    task_id='gold_transformation',
    bash_command=f"cd \"{PROJECT_ROOT}\" && export PYTHONPATH=\"{PROJECT_ROOT}/data-pipeline\" && export PYSPARK_PYTHON=\"{VENV_PYTHON}\" && export PYSPARK_DRIVER_PYTHON=\"{VENV_PYTHON}\" && \"{VENV_PYTHON}\" -m gold.main",
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

t_duckdb_validation = BashOperator(
    task_id='duckdb_validation',
    bash_command=f"cd \"{PROJECT_ROOT}\" && export PYTHONPATH=\"{PROJECT_ROOT}/data-pipeline\" && \"{VENV_PYTHON}\" data-pipeline/validate_analytics.py",
    dag=dag
)

t_resource_profiling = PythonOperator(
    task_id='resource_profiling',
    python_callable=_resource_profiling,
    dag=dag
)

# Set dependencies
t_check_infra >> t_produce_events >> t_bronze_ingestion >> t_silver_transformation >> t_gold_transformation >> t_duckdb_validation >> t_resource_profiling
