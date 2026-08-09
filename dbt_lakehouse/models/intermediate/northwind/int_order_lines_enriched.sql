-- Silver, intermediate. This is where business rules live.
--
-- The rule below -- how a line's revenue is actually computed from price,
-- quantity and discount -- is written exactly once, here. Both facts that
-- follow (`fct_order_line` and `fct_orders`) consume it, so they can never
-- drift apart. Putting this arithmetic in the facts instead is the single
-- most common way a warehouse ends up with two different "revenue" numbers.
--
-- Materialised as `ephemeral` (see dbt_project.yml): it compiles into a CTE of
-- whatever references it and never reaches the metastore, so Superset cannot
-- accidentally build a dashboard on half-finished logic.

with order_details as (

    select * from {{ ref('stg_northwind__order_details') }}

),

orders as (

    select * from {{ ref('stg_northwind__orders') }}

),

joined as (

    select
        order_details.order_id,
        order_details.product_id,

        -- order header attributes, carried down to the line grain
        orders.customer_id,
        orders.employee_id,
        orders.shipper_id,
        orders.ordered_at,
        orders.required_at,
        orders.shipped_at,
        orders.freight_amount,
        orders.ship_country,

        order_details.unit_price,
        order_details.quantity,
        order_details.discount_pct,

        -- the business rule
        cast(order_details.unit_price * order_details.quantity
             as decimal(12, 2))                                     as gross_amount,
        cast(order_details.unit_price * order_details.quantity * order_details.discount_pct
             as decimal(12, 2))                                     as discount_amount,
        cast(order_details.unit_price * order_details.quantity * (1 - order_details.discount_pct)
             as decimal(12, 2))                                     as net_amount

    from order_details
    inner join orders
        on order_details.order_id = orders.order_id

)

select * from joined
