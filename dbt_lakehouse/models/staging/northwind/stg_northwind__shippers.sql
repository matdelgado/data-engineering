with source as (

    select * from {{ source('northwind', 'shippers') }}

),

renamed as (

    select
        cast(shipperID as int)                  as shipper_id,
        companyName                             as shipper_name,
        phone                                   as shipper_phone

    from source

)

select * from renamed
