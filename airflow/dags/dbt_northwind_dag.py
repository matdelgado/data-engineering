"""Build the Northwind medallion DAG with dbt, one layer at a time.

Two things here are deliberately different from the older
`dbt_run_lakehouse_project` DAG:

1. **`dbt build` instead of `dbt run` + `dbt test`.** `build` tests each model
   immediately after creating it and stops at the first failure. `run` then
   `test` publishes every table first and only complains afterwards -- which
   means broken data sits in gold, visible to Superset, until someone reads
   the logs.

2. **One task per layer**, selected by tag, instead of one monolithic command.
   The Airflow graph then mirrors the dbt graph, so a failure tells you which
   medallion layer broke without opening the logs.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DBT_DIR = "/home/airflow/dbt_lakehouse"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 1),
    "retries": 0,
}

with DAG(
    dag_id="dbt_build_northwind",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Build the Northwind silver and gold layers with dbt",
    tags=["dbt", "northwind", "silver", "gold"],
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
        doc_md="Installs dbt_utils into dbt_packages/ (gitignored, so a fresh clone needs this).",
    )

    # Fails fast if a bronze table is missing from the catalog, with a much
    # clearer message than a downstream model blowing up on a missing relation.
    dbt_source_check = BashOperator(
        task_id="dbt_source_check",
        bash_command=f"cd {DBT_DIR} && dbt test --select source:northwind",
        doc_md="Checks the bronze layer landed. Run `northwind_ingest` first if this fails.",
    )

    dbt_build_silver = BashOperator(
        task_id="dbt_build_silver",
        bash_command=f"cd {DBT_DIR} && dbt build --select tag:silver",
        doc_md="Builds and tests the `stg_` models into schema `silver` (bucket s3a://silver).",
    )

    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=f"cd {DBT_DIR} && dbt build --select tag:gold",
        doc_md="Builds and tests the `dim_`/`fct_` models into schema `mart_sales` (bucket s3a://gold).",
    )

    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"cd {DBT_DIR} && dbt docs generate",
        doc_md="Regenerates the lineage graph served on http://localhost:8091.",
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> dbt_deps
        >> dbt_source_check
        >> dbt_build_silver
        >> dbt_build_gold
        >> dbt_docs
        >> end
    )
