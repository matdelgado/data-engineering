"""Bronze ingestion for the Northwind dataset.

Only the EL half lives here. Everything from silver onwards is dbt's job --
see the `dbt_build_northwind` DAG, which this one hands off to.

The two tasks map to the two things that have to happen before dbt can see any
data: the bytes have to land in the bronze bucket, and the Thrift server's
catalog has to be told they exist.
"""

from datetime import datetime
import sys

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# The spark/ directory is mounted at /app (see docker-compose.yml)
sys.path.append('/app')

from ingest_northwind import register as register_bronze_tables
from ingest_northwind import run as ingest_northwind_run

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 0,
}

with DAG(
    'northwind_ingest',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Land the Northwind CSVs in bronze as Delta and register them in the catalog',
    tags=['northwind', 'bronze', 'ingestion'],
) as dag:

    start = EmptyOperator(task_id='start')

    land_bronze = PythonOperator(
        task_id='land_bronze',
        python_callable=ingest_northwind_run,
        doc_md=(
            'Downloads the eight Northwind CSVs and writes them to '
            '`s3a://bronze/warehouse/northwind/` as Delta, every column as a '
            'string. No casting, no renaming -- bronze is a faithful copy.'
        ),
    )

    register_catalog = PythonOperator(
        task_id='register_catalog',
        python_callable=register_bronze_tables,
        doc_md=(
            'Registers the bronze Delta tables as `bronze.northwind_*` in the '
            'Thrift server catalog. The ingestion job writes through its own '
            'Spark session with its own metastore, so without this step dbt '
            'cannot see a thing.'
        ),
    )

    end = EmptyOperator(task_id='end')

    start >> land_bronze >> register_catalog >> end
