-- Singular test: a plain SELECT that must return zero rows to pass.
--
-- Generic tests (unique, not_null, relationships) check structure. Singular
-- tests check *business* invariants, and this is the one that matters most in
-- this project: fct_orders and fct_order_line are built at different grains
-- from the same intermediate model, so their revenue must reconcile exactly.
--
-- If someone later "fixes" the discount formula in only one of the two facts,
-- this test fails and the gold layer never gets published -- which is why the
-- Airflow DAG uses `dbt build` (test-as-you-go) instead of run-everything-
-- then-test-at-the-end.

with order_lines as (

    select
        order_id,
        sum(net_amount)     as net_amount,
        sum(gross_amount)   as gross_amount,
        sum(quantity)       as total_quantity
    from {{ ref('fct_order_line') }}
    group by order_id

),

orders as (

    select
        order_id,
        net_amount,
        gross_amount,
        total_quantity
    from {{ ref('fct_orders') }}

),

compared as (

    select
        orders.order_id,
        orders.net_amount           as order_net_amount,
        order_lines.net_amount      as line_net_amount
    from orders
    full outer join order_lines
        on orders.order_id = order_lines.order_id
    where orders.order_id is null
       or order_lines.order_id is null
       or orders.net_amount is distinct from order_lines.net_amount
       or orders.gross_amount is distinct from order_lines.gross_amount
       or orders.total_quantity is distinct from order_lines.total_quantity

)

select * from compared
