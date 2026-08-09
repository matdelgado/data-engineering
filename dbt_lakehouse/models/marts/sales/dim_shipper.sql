-- Gold, mart_sales. A small dimension straight off staging -- no intermediate
-- model needed, because there is no join and no business rule to apply.

with shippers as (

    select * from {{ ref('stg_northwind__shippers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['shipper_id']) }} as shipper_key,

        shipper_id,
        shipper_name,
        shipper_phone

    from shippers

)

select * from final
