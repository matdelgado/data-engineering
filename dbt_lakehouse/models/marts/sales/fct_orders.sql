-- Gold, mart_sales.
--
-- GRAIN: one row per order.
--
-- This is the lesson that makes the whole DAG worth building: the same
-- intermediate model feeds two facts at two different grains. Freight is the
-- reason both are needed -- it is charged per *order*, so it is additive here
-- and would be double-counted if it were pushed down to the line grain in
-- fct_order_line. Revenue is the opposite: it belongs on the line, and is
-- rolled up here.
--
-- Neither fact is "the right one". They answer different questions, and the
-- singular test in tests/ proves they agree on the numbers they share.

with order_lines as (

    select * from {{ ref('int_order_lines_enriched') }}

),

aggregated as (

    select
        order_id,
        customer_id,
        employee_id,
        shipper_id,
        ordered_at,
        required_at,
        shipped_at,
        ship_country,
        freight_amount,

        count(*)                    as line_count,
        sum(quantity)               as total_quantity,
        sum(gross_amount)           as gross_amount,
        sum(discount_amount)        as discount_amount,
        sum(net_amount)             as net_amount

    from order_lines
    group by
        order_id,
        customer_id,
        employee_id,
        shipper_id,
        ordered_at,
        required_at,
        shipped_at,
        ship_country,
        freight_amount

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id']) }}     as order_key,

        -- foreign keys
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}  as customer_key,
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }}  as employee_key,
        {{ dbt_utils.generate_surrogate_key(['shipper_id']) }}   as shipper_key,
        cast(date_format(ordered_at, 'yyyyMMdd') as int)         as order_date_key,
        cast(date_format(shipped_at, 'yyyyMMdd') as int)         as shipped_date_key,

        -- degenerate dimension
        order_id,

        -- order-level attributes
        ship_country,
        datediff(shipped_at, ordered_at)                         as days_to_ship,
        case
            when shipped_at is null then false
            when shipped_at <= required_at then true
            else false
        end                                                      as is_shipped_on_time,

        -- additive measures
        line_count,
        total_quantity,
        gross_amount,
        discount_amount,
        net_amount,
        -- additive at *this* grain only; see the header comment
        freight_amount,
        cast(net_amount + freight_amount as decimal(12, 2))      as total_amount

    from aggregated

)

select * from final
