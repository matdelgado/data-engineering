"""Bronze ingestion for the Northwind dataset.

This is the *EL* half of the pipeline: it lands the raw Northwind CSVs in the
bronze layer as Delta tables and registers them in the Thrift server's catalog.
Everything after this point is done by dbt (see dbt_lakehouse/models/).

Two deliberate choices worth explaining to students:

1. **Bronze keeps every column as a string.** No casting, no renaming, no
   dropping. Bronze is a faithful copy of the source; the moment you start
   fixing types you are already in silver. The casts live in the `stg_` models.

2. **The CSVs are parsed on the driver, not by Spark.** The whole dataset is
   ~200 KB, so `spark.createDataFrame` on a driver-side list is far simpler
   than shipping files to executors, and it keeps the job free of any
   dependency on shared volumes or S3 upload libraries.
"""

import csv
import io
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

# Public, no-auth mirror of the Microsoft Northwind sample database.
SOURCE_URL = (
    "https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples"
    "/master/examples/northwind/data/csv"
)

# The eight tables the dbt DAG actually consumes. The repository also ships
# regions/territories/employee_territories, which are left out on purpose:
# they only add a bridge table and no new modelling lesson.
TABLES = [
    "categories",
    "customers",
    "employees",
    "order_details",
    "orders",
    "products",
    "shippers",
    "suppliers",
]

BRONZE_ROOT = "s3a://bronze/warehouse/northwind"
BRONZE_SCHEMA = "bronze"
# The schema is created with an explicit LOCATION so that it reports the bronze
# bucket rather than inheriting the Thrift server's warehouse dir, which points
# at s3a://gold/warehouse. Same invariant the `spark__create_schema` macro
# enforces for the schemas dbt owns (silver, mart_sales) -- see
# `layer_locations` in dbt_lakehouse/dbt_project.yml.
BRONZE_SCHEMA_LOCATION = "s3a://bronze/warehouse"
TABLE_PREFIX = "northwind_"

THRIFT_HOST = "spark-thrift-server"
THRIFT_PORT = 10000


def _spark_session():
    return (
        SparkSession.builder.appName("ingest_northwind")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "io.delta:delta-core_2.12:2.4.0",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]
            ),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def _download(table):
    """Return (header, rows) for one Northwind CSV, everything as text."""
    url = f"{SOURCE_URL}/{table}.csv"
    with urllib.request.urlopen(url, timeout=60) as response:
        body = response.read().decode("utf-8-sig")

    reader = csv.reader(io.StringIO(body))
    header = next(reader)
    # The export writes the literal string "NULL" for missing values. Bronze
    # keeps it as-is; `nullif(col, 'NULL')` in the staging models cleans it up.
    rows = [tuple(row) for row in reader if row]
    return header, rows


def run():
    """Download every Northwind table and write it to bronze as Delta."""
    spark = _spark_session()
    try:
        for table in TABLES:
            header, rows = _download(table)
            schema = StructType([StructField(c, StringType(), True) for c in header])
            df = spark.createDataFrame(rows, schema)

            target = f"{BRONZE_ROOT}/{table}"
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).save(target)
            print(f"bronze <- {table}: {len(rows)} rows -> {target}")
    finally:
        spark.stop()


def register():
    """Register the bronze Delta tables in the Thrift server's catalog.

    The ingestion job above runs in its own Spark session with its own
    metastore, so the Thrift server -- which is what dbt and Superset connect
    to -- does not know these tables exist yet. This is the same problem the
    `scripts/update_hive_metastore.sh` step solves for the sample pipeline,
    automated here as an Airflow task.

    `CREATE TABLE ... LOCATION` makes an *external* table, so dropping it only
    removes the catalog entry and never the Delta files. That is what makes
    this task safe to re-run.
    """
    from pyhive import hive

    connection = hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT, username="airflow")
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA} "
            f"LOCATION '{BRONZE_SCHEMA_LOCATION}'"
        )
        for table in TABLES:
            qualified = f"{BRONZE_SCHEMA}.{TABLE_PREFIX}{table}"
            cursor.execute(f"DROP TABLE IF EXISTS {qualified}")
            cursor.execute(
                f"CREATE TABLE {qualified} USING DELTA "
                f"LOCATION '{BRONZE_ROOT}/{table}'"
            )
            print(f"catalog <- {qualified}")
    finally:
        cursor.close()
        connection.close()
