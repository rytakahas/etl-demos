from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from include.load_raw_to_bq import load_csv_to_bq

DEFAULT_ARGS = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bank_etl_dag",
    default_args=DEFAULT_ARGS,
    schedule="@daily",  # new Airflow syntax
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bank", "etl", "dbt"],
) as dag:

    load_customers_raw = PythonOperator(
        task_id="load_customers_raw",
        python_callable=load_csv_to_bq,
        op_kwargs={
            "project_id": "vivid-layout-453307-p4",
            "dataset_id": "ryoji_raw_demos",
            "table_id": "customers_raw",
            "csv_path": "/usr/local/airflow/data/customers.csv",
        },
    )

  # load_loans_raw = PythonOperator(
  #     task_id="load_loans_raw",
  #     python_callable=load_csv_to_bq,
  #     op_kwargs={
  #         "project_id": "vivid-layout-453307-p4",
  #         "dataset_id": "ryoji_raw_demos",
  #         "table_id": "loan_applications_raw",
  #         "csv_path": "/usr/local/airflow/data/auto_loan_default.csv",
  #     },
  # )
    load_loans_raw = PythonOperator(
        task_id="load_loans_raw",
        python_callable=load_csv_to_bq,
        op_kwargs={
            "project_id": "vivid-layout-453307-p4",
            "dataset_id": "ryoji_raw_demos",
            "table_id": "loan_applications_raw",
            "csv_path": "/usr/local/airflow/data/vehicle_loans_train.csv",
            "sanitize_header": True,
        },
    )



    load_payments_raw = PythonOperator(
        task_id="load_payments_raw",
        python_callable=load_csv_to_bq,
        op_kwargs={
            "project_id": "vivid-layout-453307-p4",
            "dataset_id": "ryoji_raw_demos",
            "table_id": "payments_raw",
            "csv_path": "/usr/local/airflow/data/payments.csv",
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /usr/local/airflow/dbt && "
            "dbt run --profiles-dir . --project-dir ."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /usr/local/airflow/dbt && "
            "dbt test --profiles-dir . --project-dir ."
        ),
    )

    [load_customers_raw, load_loans_raw, load_payments_raw] >> dbt_run >> dbt_test

