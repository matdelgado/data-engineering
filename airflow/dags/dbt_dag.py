from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = "/home/airflow/dbt_lakehouse"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 1),
    "retries": 0,
}

with DAG(
    dag_id="dbt_run_lakehouse_project",
    default_args=default_args,
    #schedule_interval="0 10 * * *",  # every day at 10:00 AM
    schedule_interval=None,
    catchup=False,
    description="Run the entire dbt lakehouse project",
    tags=["dbt", "lakehouse"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_all",
        bash_command=f"cd {DBT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"cd {DBT_DIR} && dbt docs generate",
    )

    dbt_run >> dbt_test >> dbt_docs
