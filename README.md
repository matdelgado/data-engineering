# Data Engineering

Welcome to the data engineering project!

## Prerequisites

Before getting started, you will need to install the following software on your machine:

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)  
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)  
* [VSCode](https://code.visualstudio.com/)

Since the installation process depends on your operating system, you’ll need to handle this part on your own.
However, I've added some guidance for [Windows](./_support/windows.md), [macOS](./_support/macos.md) and [Ubuntu](./_support/ubuntu.md).

You also need to create a GitHub account to follow along with the project. Create your account at [https://github.com/](https://github.com/).

## Cloning the Project Repository

First, you need to access the original project repository at https://github.com/weslleymoura/data-engineering and create a **fork**. This will make a copy of the project in your own GitHub account (as a new repository).

<img src="_support/git-fork.png" width="400">

**After forking**, open your terminal and navigate to the **directory where you want to save the project** (throughout the project, we will refer to this directory as the **working dir**).

Next, clone your forked project:

```
git clone <<your-repository-url>>
```

To get your project URL, go to the GitHub repository you just forked (in your GitHub account) and copy the following address (HTTPS):

<img src="_support/git-clone.png" width="400">


## Starting the project

Please, execute the following commands to initiate the project.

#### Working directory

Before you start, make sure you are inside the project directory (data-engineering).

#### Grant permissions

Make sure you granted full access on Airflow and DBT folders

```
sudo chmod -R 777 airflow/
sudo chmod -R 777 dbt_lakehouse/
```

#### Build all services

Use the following command to build all services.

```
docker compose up -d --build
```

The first run downloads several GB of images and builds three of them, so it can take
15-30 minutes depending on your connection. Give Docker Desktop at least **8 GB of RAM**
(Settings → Resources), otherwise Spark and Superset will be killed on startup.

Follow the progress with:

```
docker compose ps
docker compose logs -f airflow
```

#### Checking if everything is okay

Your project is correct if you are able to access the following links from your localhost.

| Service | URL | User | Password |
| --- | --- | --- | --- |
| MinIO Console UI | http://localhost:9001 | `minio` | `minio123` |
| Airflow Web UI | http://localhost:8080 | `admin` | `admin` |
| Spark Master UI | http://localhost:8081 | – | – |
| Apache Superset UI | http://localhost:8088 | `admin` | `admin` |

These are throwaway credentials for a local teaching environment — never reuse them
outside of it.

## Filling the lakehouse

At this point every service is running, but **no data has been processed yet**: the
lakehouse is empty apart from the raw CSV that was uploaded to the bronze bucket.

The steps below are what actually build the lakehouse, and they **must run in this order**
— each one depends on the previous.

#### 1. Run the ingestion pipeline

Open the [Airflow Web UI](http://localhost:8080) (`admin` / `admin`), find the
`lakehouse_pipeline` DAG, **unpause it** with the toggle on the left, then hit the ▶ button
to trigger a run.

> DAGs start paused on purpose (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`), so nothing
> runs behind your back. You have to unpause them yourself.

Open the run and follow the graph until both tasks are green:

- `bronze_to_silver` reads `sample_data.csv` from the bronze bucket and writes it as a
  Delta table to silver
- `silver_to_gold` aggregates it per customer and writes the result to gold

You can watch the job execute in the [Spark Master UI](http://localhost:8081), and see the
new Delta files appear in the [MinIO Console](http://localhost:9001).

#### 2. Register the gold table in the data catalog

```
docker exec -it spark-master bash -c "/scripts/update_hive_metastore.sh"
```

The pipeline writes through its own Spark session, which keeps its **own** metastore — the
Thrift server does not know about the gold table yet. This step registers it in the Thrift
server's catalog, which is what Superset and DBT connect to.

> ⚠️ Order matters. Running this **before** the pipeline registers a table pointing at an
> empty location: it succeeds without any error message, and only blows up later, on the
> first query.

#### 3. Run the DBT project

Back in the Airflow UI, unpause and trigger `dbt_run_lakehouse_project`. Its three tasks run
`dbt run`, `dbt test` and `dbt docs generate`, building the `marts` layer on top of the gold
table.

#### 4. Confirm the lakehouse is built

```
docker exec -it spark-master bash -c "beeline -u jdbc:hive2://spark-thrift-server:10000 -e 'SELECT SUM(total_amount) FROM marts.fct_summary;'"
```

You should get a single number back. If you do, the whole chain works end to end:
MinIO → Spark → Delta → Hive metastore → DBT.

## Building the Northwind star schema

The pipeline above is a one-table warm-up. This second pipeline is the real thing: a
dimensional model built with DBT across all three medallion layers, using the
[Northwind sample database](https://github.com/graphql-compose/graphql-compose-examples/tree/master/examples/northwind/data/csv)
— eight tables, ~200 KB, downloaded straight from GitHub with no account needed.

### How the layers map

Medallion (bronze/silver/gold) describes *storage*. DBT's staging/intermediate/marts
describes *transformation*. They line up, but they are not the same vocabulary:

| Medallion | Bucket | Schema | DBT folder | Prefix | Materialisation |
| --- | --- | --- | --- | --- | --- |
| Bronze | `s3a://bronze` | `bronze` | `sources.yml` — **not** a model | — | Delta, written by Spark |
| Silver | `s3a://silver` | `silver` | `models/staging/northwind` | `stg_` | table |
| Silver | — | — | `models/intermediate/northwind` | `int_` | ephemeral |
| Gold | `s3a://gold` | `mart_sales` | `models/marts/sales` | `dim_` / `fct_` | table |

Four decisions in that table are worth understanding, because they are the ones people
usually get wrong:

* **Bronze is a source, not a model.** DBT does not ingest — it starts where the data has
  already landed. A `raw_orders` model that only does `select * from bronze.orders` is a
  second copy of the same bytes. What you want is `source()`, which still shows up as a
  node in the lineage graph.
* **The bucket is attached to the schema, not to each table.** The Thrift server is started
  with `spark.sql.warehouse.dir=s3a://gold/warehouse`, so by default *every* schema is
  registered inside the gold bucket no matter what it is called. DBT's own
  `spark__create_schema` emits a bare `create schema if not exists`, with no LOCATION, so
  it cannot fix this. The project overrides that macro — see
  [macros/spark_create_schema.sql](dbt_lakehouse/macros/spark_create_schema.sql) — and
  reads the bucket from a single `layer_locations` mapping in `dbt_project.yml`. Every
  table created in the schema then inherits the right bucket automatically. Details in
  [Why the schema owns the bucket](#why-the-schema-owns-the-bucket) below.
* **Silver is materialised as tables, not the DBT default of views.** In a lakehouse the
  silver layer has to be physically readable by Trino/Superset/Spark, and a view here
  would re-read the bronze Delta on every query.
* **`int_` models are ephemeral.** They compile into CTEs of whatever references them and
  never reach the metastore, which is what stops Superset from building a dashboard on
  half-finished business logic. Confirm it yourself: `show schemas` has no `intermediate`.

### The DAG

```
bronze (source)          silver (stg_)                  silver (int_, ephemeral)       gold (mart_sales)
─────────────────        ───────────────────────        ────────────────────────       ──────────────────
orders            ──►    stg_northwind__orders     ──┐
order_details     ──►    stg_northwind__order_details ├─► int_order_lines_enriched ──┬─► fct_order_line
products          ──►    stg_northwind__products   ──┤                               │
categories        ──►    stg_northwind__categories ──┼─► int_products_categorized ──┐└─► fct_orders
suppliers         ──►    stg_northwind__suppliers  ──┘                              └───► dim_product
customers         ──►    stg_northwind__customers  ──────────────────────────────────────► dim_customer
employees         ──►    stg_northwind__employees  ──────────────────────────────────────► dim_employee
shippers          ──►    stg_northwind__shippers   ──────────────────────────────────────► dim_shipper
                         (generated, no source) ────────────────────────────────────────►  dim_date
```

Two facts are built from the same intermediate model at **two different grains**, which is
the lesson the whole DAG exists to teach:

* `fct_order_line` — one row per product on an order. The revenue fact.
* `fct_orders` — one row per order. Carries `freight_amount`, which is charged per order
  and would be double-counted if it were pushed down to the line grain.

Neither is "the right one". They answer different questions, and
`tests/assert_facts_agree_on_revenue.sql` proves they agree on the measures they share.

### 1. Land the Northwind data in bronze

In the [Airflow Web UI](http://localhost:8080), unpause and trigger `northwind_ingest`.

* `land_bronze` downloads the eight CSVs and writes them to
  `s3a://bronze/warehouse/northwind/` as Delta, **every column as a string** — bronze is a
  faithful copy of the source, and casting is the staging layer's job.
* `register_catalog` registers them as `bronze.northwind_*` in the Thrift server's catalog.
  This is the same gotcha as step 2 of the first pipeline: the ingestion job writes through
  its own Spark session with its own metastore, so without this task DBT cannot see a
  thing. Here it is automated instead of being a manual `docker exec`.

### 2. Build silver and gold with DBT

Unpause and trigger `dbt_build_northwind`. It runs `dbt deps`, checks the bronze sources
arrived, then builds one medallion layer per task so the Airflow graph mirrors the DBT
graph.

It uses `dbt build` rather than `dbt run` followed by `dbt test`. `build` tests each model
immediately after creating it and stops at the first failure; `run` then `test` publishes
every table first and only complains afterwards — which means broken data sits in gold,
visible to Superset, until somebody reads the logs.

Expect 8 models + 35 tests in silver, and 7 models + 56 tests in gold.

### 3. Confirm the star schema works

```
docker exec -it spark-master bash -c "beeline -u jdbc:hive2://spark-thrift-server:10000 -e \"SELECT d.year_month, COUNT(*) AS orders, SUM(f.net_amount) AS revenue FROM mart_sales.fct_orders f JOIN mart_sales.dim_date d ON f.order_date_key = d.date_key GROUP BY d.year_month ORDER BY d.year_month LIMIT 6;\""
```

```
+-------------+---------+-----------+
| year_month  | orders  |  revenue  |
+-------------+---------+-----------+
| 1996-07     | 22      | 27861.90  |
| 1996-08     | 25      | 25485.28  |
| 1996-09     | 23      | 26381.40  |
...
```

You can also check the physical medallion split in the [MinIO Console](http://localhost:9001):
`stg_*` tables under the **silver** bucket, `dim_*`/`fct_*` under **gold**, and nothing at
all for the `int_` models. The catalog agrees with the buckets, which is not free — see
[Why the schema owns the bucket](#why-the-schema-owns-the-bucket):

```
docker exec -it spark-master bash -c "beeline -u jdbc:hive2://spark-thrift-server:10000 -e 'DESCRIBE DATABASE silver; DESCRIBE DATABASE mart_sales;'"
```

```
| Location  | s3a://silver/warehouse |
| Location  | s3a://gold/warehouse   |
```

The lineage graph is worth opening too — regenerate the docs (see below) and browse to
http://localhost:8091.

### DBT conventions used here

Worth reading alongside the models, since every one of these is a deliberate choice:

1. **Bronze is never a DBT model.** Declare it in `sources.yml`.
2. **One staging model per source table, 1:1, no joins.** Renaming, casting and dropping
   junk columns only. Business rules belong in `int_`.
3. **`stg_<source>__<entity>`** — the double underscore separates source from entity, so
   two systems that both have an `orders` table never collide.
4. **`int_` names describe the verb**: `int_order_lines_enriched`, never `int_orders_2`.
5. **Declare the grain of every fact** in its description, and prove it with a `unique`
   test on the key at that grain.
6. **Surrogate keys** via `dbt_utils.generate_surrogate_key`, so facts never carry an ugly
   natural key like `"ALFKI"`. `dim_date` is the one exception: `yyyyMMdd` is sortable and
   readable inside the fact table.
7. **`source()` only in staging.** Everything else uses `ref()`.
8. **Tests get stricter going up.** Staging checks primary keys; marts check grain
   uniqueness *and* `relationships` from every fact FK to its dimension — in practice the
   test that catches the most real bugs, because a broken FK produces silently missing
   rows in a dashboard rather than an error.
9. **Configure the folder, not the model.** `dbt_project.yml` sets `+materialized`,
   `+schema` and `+tags` per directory; models override only exceptions.
10. **The prefix is a layer contract.** `dim_`/`fct_` only in gold, `stg_` only in silver.

### Why the schema owns the bucket

This is the piece that makes the medallion split real rather than decorative, and it is
worth walking through because the failure mode is silent.

The Thrift server starts with `spark.sql.warehouse.dir=s3a://gold/warehouse`. Any schema
created without an explicit LOCATION inherits it. You can still see this on the old
sample schema, which predates the fix:

```
docker exec -it spark-master bash -c "beeline -u jdbc:hive2://spark-thrift-server:10000 -e 'DESCRIBE DATABASE marts;'"
```

```
| Location  | s3a://gold/warehouse/marts.db |
```

The obvious fix is `+location_root` on each model folder, which is what this project did
first. It works, but only for tables DBT itself creates: the *schema* stays registered
under the gold bucket, so `DESCRIBE DATABASE silver` reports gold while the tables inside
it report silver. The metadata contradicts the data, and anything created outside DBT — a
Spark job, a `CREATE TABLE` in beeline, `dbt seed`, `dbt snapshot` — quietly lands in the
wrong bucket.

Attaching the location to the schema fixes it at the source, because managed tables
inherit their schema's location. DBT does not do this on its own, so the project
overrides the adapter macro that creates schemas:

```sql
-- macros/spark_create_schema.sql
{% macro spark__create_schema(relation) -%}
    {%- set location = var('layer_locations', {}).get(relation.schema | string) -%}
    {%- call statement('create_schema') -%}
        create schema if not exists {{ relation }}
        {%- if location %} location '{{ location }}'{%- endif %}
    {%- endcall -%}
{% endmacro %}
```

```yaml
# dbt_project.yml -- the only place a bucket is named
vars:
  layer_locations:
    bronze: s3a://bronze/warehouse
    silver: s3a://silver/warehouse
    mart_sales: s3a://gold/warehouse
```

Overriding `<adapter>__<macro_name>` like this is the escape hatch DBT gives you when the
adapter does not do what your platform needs. The result:

```
DESCRIBE DATABASE silver                     -> Location: s3a://silver/warehouse
DESCRIBE FORMATTED silver.stg_northwind__orders
                                             -> Type: MANAGED
                                             -> Location: s3a://silver/warehouse/stg_northwind__orders
```

And the invariant now holds for things DBT never touched:

```
CREATE TABLE silver.made_by_hand USING DELTA AS SELECT 1 AS x;
-> Location: s3a://silver/warehouse/made_by_hand
```

**Migrating an existing schema.** The Hive metastore cannot change a schema's location
after creation:

```
ALTER DATABASE silver SET LOCATION 's3a://silver/warehouse';
-> AnalysisException: Hive metastore does not support altering database location.
```

So a schema created with the wrong location has to be dropped and rebuilt. Everything
here is reproducible from bronze, and bronze is reproducible from GitHub, so this is
safe:

```
docker exec -it spark-master bash -c "beeline -u jdbc:hive2://spark-thrift-server:10000 -e 'DROP DATABASE IF EXISTS silver CASCADE; DROP DATABASE IF EXISTS mart_sales CASCADE;'"
docker exec -it mc sh -c "mc rm --recursive --force local/silver/warehouse/northwind/; mc rm --recursive --force local/gold/warehouse/northwind/"
```

then re-trigger `dbt_build_northwind`. The `mc rm` step matters: the old tables were
EXTERNAL, so `DROP ... CASCADE` removes the catalog entries and leaves the files behind.

**What this still is not.** A real lakehouse addresses data as `catalog.schema.table`,
with the catalog being the top-level boundary (`bronze.northwind.orders`). This Spark
setup has exactly one catalog, `spark_catalog`, so bronze/silver/gold have to be schemas.
Three-level namespaces need a catalog provider — Unity Catalog, AWS Glue, an Iceberg REST
catalog — which is also why DBT's `catalog` model config does nothing here.

### Rebuilding just one layer

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse && dbt build --select tag:silver"
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse && dbt build --select tag:gold"
```

Or one model plus everything downstream of it:

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse && dbt build --select stg_northwind__orders+"
```

## Notes for project configurations (no need to execute anything)

Please, take note of the following project configurations 

#### Superset connection string

Use this connection string to connect Superset to the lakehouse.

```
hive://spark-thrift-server:10000/default
```

Once connected, you should be able to query the DBT table

```
SELECT SUM(total_amount) AS total_amount FROM marts.fct_summary
```



#### Initiating DBT Docs server

The docs are generated by the `dbt_generate_docs` task, so run the
`dbt_run_lakehouse_project` DAG first — otherwise `target/` has no `index.html` and you will
just get a directory listing. To generate them by hand instead:

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse && dbt docs generate"
```

Then serve them:

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse/target && python3 -m http.server 8091"
```

Check results at http://localhost:8091


## Support commands (run as needed)

#### Access airflow container as root user.

```
docker exec -u 0 -it airflow bash
```

#### Installing DBT package dependencies

`dbt_packages/` is not committed, so on a fresh clone this has to run once before any
other DBT command. Both DBT DAGs do it for you as their first task; you only need this if
you are running DBT by hand.

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse && dbt deps"
```

Skipping it makes every DBT command fail with *"found 1 package(s) specified in
packages.yml, but only 0 package(s) installed in dbt_packages"*.

#### Run DBT project from Airflow container
```
docker exec -it airflow bash -c "dbt run --project-dir /home/airflow/dbt_lakehouse"
```

#### Run DBT model from Airflow container
```
docker exec -it airflow bash -c "dbt run --project-dir /home/airflow/dbt_lakehouse --select models/marts/fct_summary.sql"
```

#### Connecting to the Hive metastore from spark-master

```
docker exec -it spark-master bash
beeline -u jdbc:hive2://spark-thrift-server:10000
show schemas;
show tables in default;
show tables in marts;
```

#### Shutting down docker containers

You can use the following command to destroy all services.

```
docker compose down --volumes --remove-orphans
```

> ⚠️ Use `--volumes`. Without it you get a **half reset**, which is worse than either
> extreme: the Thrift server keeps its catalog in the container filesystem, so `down`
> wipes every schema and table registration, while the MinIO data lives in a named volume
> and survives. On the next run DBT sees an empty catalog, tries to create a table, and
> finds Delta files already sitting at that path:
>
> ```
> io.delta.exceptions.MetadataChangedException: The metadata of the Delta table
> has been changed by a concurrent update. Please try the operation again.
> ```
>
> If you are already in that state, either drop the volumes and start over, or clear the
> orphaned files by hand:
>
> ```
> docker exec -it mc sh -c "mc rm --recursive --force local/gold/warehouse/; mc rm --recursive --force local/silver/warehouse/"
> ```
>
> Then re-run the DAGs in the documented order.

In case you have lost refence to the existing containers, it is possible to force a shut down.

```
docker container kill $(docker container ls -q)
```

> ⚠️ This kills **every** running container on your machine, not only this project's.

## Troubleshooting

#### Checking container logs

Whatever the problem is, the reason is almost always in the last few lines of the
service's log:

```
docker compose logs --tail=50 airflow
```

#### `Bind for 0.0.0.0:8080 failed: port is already allocated`

Another process (often an unrelated container) is already using one of the ports
this project publishes: `8080`, `8081`, `8088`, `8091`, `9000`, `9001`, `7077`,
`10000`, `10001`. Find the culprit and stop it:

```
docker ps --filter "publish=8080"
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

#### Resolving permissions issue

In case Airflow is failing because of permission issues.

```
sudo chmod -R 777 airflow/
sudo chmod -R 777 dbt_lakehouse/
```
