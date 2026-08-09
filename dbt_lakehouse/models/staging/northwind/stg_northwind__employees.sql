with source as (

    select * from {{ source('northwind', 'employees') }}

),

renamed as (

    select
        cast(employeeID as int)                 as employee_id,
        -- self-referencing FK; resolved into a manager name in dim_employee
        cast(reportsTo as int)                  as manager_employee_id,

        firstName                               as first_name,
        lastName                                as last_name,
        concat(firstName, ' ', lastName)        as employee_name,
        title                                   as job_title,
        titleOfCourtesy                         as title_of_courtesy,
        cast(birthDate as date)                 as birth_date,
        cast(hireDate as date)                  as hire_date,
        city                                    as employee_city,
        nullif(region, 'NULL')                  as employee_region,
        country                                 as employee_country

        -- `photo`, `photoPath` and `notes` are dropped: two blobs and a free
        -- text field that no dimension or fact needs.

    from source

)

select * from renamed
