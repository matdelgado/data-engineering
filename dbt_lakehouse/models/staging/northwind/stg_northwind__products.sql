with source as (

    select * from {{ source('northwind', 'products') }}

),

renamed as (

    select
        -- keys
        cast(productID as int)                  as product_id,
        cast(supplierID as int)                 as supplier_id,
        cast(categoryID as int)                 as category_id,

        -- attributes
        productName                             as product_name,
        quantityPerUnit                         as quantity_per_unit,
        cast(unitPrice as decimal(10, 2))       as list_price,
        cast(unitsInStock as int)               as units_in_stock,
        cast(unitsOnOrder as int)               as units_on_order,
        cast(reorderLevel as int)               as reorder_level,

        -- turning a 0/1 flag into a real boolean is the kind of light
        -- standardisation staging is for
        case
            when cast(discontinued as int) = 1 then true
            else false
        end                                     as is_discontinued

    from source

)

select * from renamed
