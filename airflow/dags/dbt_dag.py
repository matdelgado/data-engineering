"""Run the sample (`order_summary`) part of the dbt project.

The Northwind models are excluded on purpose. They live in the same dbt
project, but they depend on the `bronze` schema that the `northwind_ingest`
DAG creates, so a bare `dbt run` here would fail on eight staging models for
anyone who has not run that DAG yet. `--exclude tag:northwind` keeps the two
tutorials independent of each other, in either order.

See the `dbt_build_northwind` DAG for the Northwind side.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = "/home/airflow/dbt_lakehouse"
# Everything in models/{staging,intermediate,marts}/northwind and sales/
NORTHWIND = "--exclude tag:northwind"

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
        bash_command=f"cd {DBT_DIR} && dbt run {NORTHWIND}",
    )

    # `dbt_packages/` is gitignored, so a fresh clone has no packages installed.
    # dbt refuses to do anything at all while packages.yml lists a dependency
    # that is missing -- including this DAG, which does not use dbt_utils
    # itself. Without this task the very first run on a new machine fails with
    # "found 1 package(s) specified in packages.yml, but only 0 installed".
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test {NORTHWIND}",
    )

    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"cd {DBT_DIR} && dbt docs generate",
    )

    dbt_deps >> dbt_run >> dbt_test >> dbt_docs
