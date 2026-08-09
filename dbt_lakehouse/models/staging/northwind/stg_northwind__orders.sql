-- Silver. One staging model per source table, 1:1, no joins.
-- Allowed here: renaming, casting, and cleaning up source quirks.
-- Not allowed here: joins, aggregations, business rules. Those live in `int_`.

with source as (

    select * from {{ source('northwind', 'orders') }}

),

renamed as (

    select
        -- keys
        cast(orderID as int)                    as order_id,
        customerID                              as customer_id,
        cast(employeeID as int)                 as employee_id,
        cast(shipVia as int)                    as shipper_id,

        -- timestamps
        cast(orderDate as timestamp)            as ordered_at,
        cast(requiredDate as timestamp)         as required_at,
        cast(shippedDate as timestamp)          as shipped_at,

        -- measures
        cast(freight as decimal(10, 2))         as freight_amount,

        -- shipping attributes
        shipName                                as ship_name,
        shipAddress                             as ship_address,
        shipCity                                as ship_city,
        -- the CSV export writes the literal text "NULL" for missing values
        nullif(shipRegion, 'NULL')              as ship_region,
        nullif(shipPostalCode, 'NULL')          as ship_postal_code,
        shipCountry                             as ship_country

    from source

)

select * from renamed
