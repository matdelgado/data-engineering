with source as (

    select * from {{ source('northwind', 'categories') }}

),

renamed as (

    select
        cast(categoryID as int)                 as category_id,
        categoryName                            as category_name,
        description                             as category_description

        -- `picture` is dropped on purpose: it is a hex-encoded MS Access
        -- bitmap that nothing downstream can use, and it is by far the widest
        -- column in the table. Throwing away useless source columns is one of
        -- the main reasons the staging layer exists.

    from source

)

select * from renamed
