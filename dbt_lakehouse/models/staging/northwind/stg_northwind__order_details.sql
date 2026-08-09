with source as (

    select * from {{ source('northwind', 'order_details') }}

),

renamed as (

    select
        -- composite key: an order line is identified by order + product
        cast(orderID as int)                    as order_id,
        cast(productID as int)                  as product_id,

        -- measures. The price is snapshotted on the line, so it can differ
        -- from the product's current list price -- that is a feature, not a bug.
        cast(unitPrice as decimal(10, 2))       as unit_price,
        cast(quantity as int)                   as quantity,
        cast(discount as decimal(5, 4))         as discount_pct

    from source

)

select * from renamed
