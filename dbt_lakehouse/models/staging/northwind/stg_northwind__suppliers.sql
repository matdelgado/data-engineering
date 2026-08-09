with source as (

    select * from {{ source('northwind', 'suppliers') }}

),

renamed as (

    select
        cast(supplierID as int)                 as supplier_id,
        companyName                             as supplier_name,
        contactName                             as supplier_contact_name,
        contactTitle                            as supplier_contact_title,
        city                                    as supplier_city,
        nullif(region, 'NULL')                  as supplier_region,
        country                                 as supplier_country

    from source

)

select * from renamed
