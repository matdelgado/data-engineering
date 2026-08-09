-- Gold, mart_sales. Resolves the self-referencing `reportsTo` hierarchy into
-- a flat manager name, so "sales by manager" does not require a recursive CTE
-- in every dashboard.

with employees as (

    select * from {{ ref('stg_northwind__employees') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['employees.employee_id']) }} as employee_key,

        employees.employee_id,
        employees.employee_name,
        employees.first_name,
        employees.last_name,
        employees.job_title,
        employees.title_of_courtesy,
        employees.birth_date,
        employees.hire_date,
        employees.employee_city,
        employees.employee_region,
        employees.employee_country,

        employees.manager_employee_id,
        managers.employee_name                                            as manager_name

    from employees
    left join employees as managers
        on employees.manager_employee_id = managers.employee_id

)

select * from final
