with source as (

    select * from {{ source('northwind', 'customers') }}

),

renamed as (

    select
        -- natural key. Note it is a 5-letter code ("ALFKI"), not an integer:
        -- exactly why the marts hash it into a surrogate key instead of
        -- carrying the source key into the fact tables.
        customerID                              as customer_id,

        companyName                             as customer_name,
        contactName                             as contact_name,
        contactTitle                            as contact_title,
        address                                 as customer_address,
        city                                    as customer_city,
        nullif(region, 'NULL')                  as customer_region,
        nullif(postalCode, 'NULL')              as customer_postal_code,
        country                                 as customer_country,
        phone                                   as customer_phone

    from source

)

select * from renamed
