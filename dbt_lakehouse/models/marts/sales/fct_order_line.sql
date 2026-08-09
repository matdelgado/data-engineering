-- Gold, mart_sales.
--
-- GRAIN: one row per product on an order (order_id + product_id).
-- Declaring the grain first, before writing a line of SQL, is the whole job of
-- dimensional modelling. Everything below follows from it: which keys belong
-- here, and which measures are additive at this grain.
--
-- A fact table holds three things and nothing else:
--   * foreign keys to dimensions
--   * additive measures
--   * degenerate dimensions (source identifiers with no dimension of their own)
--
-- Note that the surrogate keys are rebuilt here with the same expression the
-- dimensions use. That is not duplication for its own sake: it means the fact
-- never has to join to a dimension just to look up a key.

with order_lines as (

    select * from {{ ref('int_order_lines_enriched') }}

),

final as (

    select
        -- primary key of the fact, at the declared grain
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_line_key,

        -- foreign keys
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}            as customer_key,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }}             as product_key,
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }}            as employee_key,
        {{ dbt_utils.generate_surrogate_key(['shipper_id']) }}             as shipper_key,
        cast(date_format(ordered_at, 'yyyyMMdd') as int)                   as order_date_key,
        cast(date_format(shipped_at, 'yyyyMMdd') as int)                   as shipped_date_key,

        -- degenerate dimensions: the source identifiers, kept so an analyst
        -- can trace a row back to the operational system
        order_id,
        product_id,

        -- additive measures
        quantity,
        gross_amount,
        discount_amount,
        net_amount,

        -- non-additive: a rate. Safe to filter and group by, never to SUM.
        -- Kept because the additive components it derives from are also here.
        unit_price,
        discount_pct

    from order_lines

)

select * from final
