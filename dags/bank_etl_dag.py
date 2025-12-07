from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, timedelta
import yaml
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from include.load_raw_to_bq import load_csv_to_bq

DEFAULT_ARGS = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def load_raw_sources_config() -> list[dict]:
    """
    Read config/raw_sources.yml and return the list of raw sources.
    """
    airflow_home = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
    config_path = airflow_home / "config" / "raw_sources.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"raw_sources.yml not found at {config_path}")
    with config_path.open() as f:
        data = yaml.safe_load(f) or {}
    raw_sources = data.get("raw_sources", [])
    if not raw_sources:
        raise ValueError("raw_sources.yml is empty or missing 'raw_sources' key")
    return raw_sources

RAW_SOURCES = load_raw_sources_config()

with DAG(
    dag_id="bank_etl_dag",
    default_args=DEFAULT_ARGS,
    schedule="@daily",  # new Airflow syntax
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bank", "etl", "dbt"],
) as dag:
    # 1) Dynamically build one load task per raw source
    load_tasks = []
    for src in RAW_SOURCES:
        task = PythonOperator(
            task_id=f"load_{src['name']}",
            python_callable=load_csv_to_bq,
            op_kwargs={
                "project_id": src["project_id"],
                "dataset_id": src["dataset_id"],
                "table_id": src["table_id"],
                "csv_path": src["csv_path"],
            },
        )
        load_tasks.append(task)
    
    # 2) dbt run + dbt test
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /usr/local/airflow/dbt && "
            "dbt run --profiles-dir . --project-dir . --full-refresh"
        ),
    )
    
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /usr/local/airflow/dbt && "
            "dbt test --profiles-dir . --project-dir ."
        ),
    )
    
    # 3) Dependencies: all loads → dbt run → dbt test
    load_tasks >> dbt_run >> dbt_test