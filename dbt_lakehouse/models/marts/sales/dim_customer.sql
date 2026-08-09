-- Gold, mart_sales.
--
-- The surrogate key is a hash of the natural key, not the natural key itself.
-- That is what lets the fact table join to the dimension with a single fixed
-- width column regardless of how ugly the source key is (here: "ALFKI"), and
-- what makes it possible to add SCD Type 2 history later without changing a
-- single fact table.

with customers as (

    select * from {{ ref('stg_northwind__customers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,

        customer_id,
        customer_name,
        contact_name,
        contact_title,
        customer_address,
        customer_city,
        customer_region,
        customer_postal_code,
        customer_country,
        customer_phone

    from customers

)

select * from final
