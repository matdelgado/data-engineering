-- Gold, mart_sales. Category and supplier are denormalised in on purpose:
-- a star schema trades storage for join-free queries, and a BI user should
-- never have to know that `category_name` lives in a different source table.

with products as (

    select * from {{ ref('int_products_categorized') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

        product_id,
        product_name,
        quantity_per_unit,
        list_price,
        units_in_stock,
        units_on_order,
        reorder_level,
        is_discontinued,

        category_id,
        category_name,
        category_description,

        supplier_id,
        supplier_name,
        supplier_city,
        supplier_country

    from products

)

select * from final
