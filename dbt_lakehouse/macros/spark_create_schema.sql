{#
    Overrides dbt-spark's own `spark__create_schema`, which emits a bare
    `create schema if not exists <name>` with no LOCATION. Without a location
    the schema inherits `spark.sql.warehouse.dir` -- which the Thrift server
    sets to s3a://gold/warehouse (see spark/Dockerfile) -- so a schema called
    `silver` would be registered inside the *gold* bucket.

    That is not a cosmetic problem. Check it on the old sample schema:

        DESCRIBE DATABASE marts
        -> Location: s3a://gold/warehouse/marts.db

    Attaching the location to the *schema* instead of to each table means any
    managed table created in it inherits the right bucket automatically --
    including tables created by Spark, by beeline, or by `dbt seed`/`dbt
    snapshot`, none of which go through a model config. One mapping
    (`layer_locations` in dbt_project.yml) instead of one setting per model.

    Overriding an adapter macro like this is the escape hatch dbt gives you
    when the adapter does not do what your platform needs. The macro name has
    to match exactly: `<adapter>__<macro_name>`.

    Caveat worth knowing: the Hive metastore cannot change a schema's location
    after the fact --

        ALTER DATABASE silver SET LOCATION '...'
        -> AnalysisException: Hive metastore does not support altering
           database location.

    -- so a schema created with the wrong location has to be dropped and
    recreated. See the README for the migration.
#}

{% macro spark__create_schema(relation) -%}
    {%- set location = var('layer_locations', {}).get(relation.schema | string) -%}
    {%- call statement('create_schema') -%}
        create schema if not exists {{ relation }}
        {%- if location %} location '{{ location }}'{%- endif %}
    {%- endcall -%}
{% endmacro %}
