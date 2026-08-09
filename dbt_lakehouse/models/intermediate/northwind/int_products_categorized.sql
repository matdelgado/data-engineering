-- Silver, intermediate. Collapses the products / categories / suppliers
-- snowflake into one flat product record.
--
-- Doing this here rather than inside dim_product keeps the dimension a thin
-- "add the surrogate key and publish" step, and leaves the join reusable if a
-- second mart ever needs product attributes.

with products as (

    select * from {{ ref('stg_northwind__products') }}

),

categories as (

    select * from {{ ref('stg_northwind__categories') }}

),

suppliers as (

    select * from {{ ref('stg_northwind__suppliers') }}

),

joined as (

    select
        products.product_id,
        products.product_name,
        products.quantity_per_unit,
        products.list_price,
        products.units_in_stock,
        products.units_on_order,
        products.reorder_level,
        products.is_discontinued,

        -- left joins, not inner: a product with a broken category FK must
        -- still show up in the dimension, otherwise the fact loses rows.
        categories.category_id,
        categories.category_name,
        categories.category_description,

        suppliers.supplier_id,
        suppliers.supplier_name,
        suppliers.supplier_city,
        suppliers.supplier_country

    from products
    left join categories
        on products.category_id = categories.category_id
    left join suppliers
        on products.supplier_id = suppliers.supplier_id

)

select * from joined
