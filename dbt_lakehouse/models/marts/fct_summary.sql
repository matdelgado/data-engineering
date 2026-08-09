-- The schema (`marts`) comes from dbt_project.yml.
--
-- This model used to carry `catalog="gold"` as well. It was a no-op: this
-- Spark setup has a single catalog (`spark_catalog`), so there is no `gold`
-- catalog to route to. The table landed in s3a://gold/warehouse/marts.db/
-- either way -- not because of the config, but because that path is the
-- Thrift server's `spark.sql.warehouse.dir`. Three-level namespaces
-- (catalog.schema.table) need a real catalog provider such as Unity Catalog,
-- AWS Glue or an Iceberg REST catalog.

SELECT
  customer_id,
  total_amount
FROM {{ source('lakehouse_gold', 'order_summary') }}
