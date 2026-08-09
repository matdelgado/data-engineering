-- Gold, mart_sales. A dimension with no source table at all.
--
-- Two things worth pointing out to students:
--   1. Not every dimension comes from the source system. A date dimension is
--      generated, and it is what lets "revenue by quarter" work without every
--      analyst re-deriving fiscal calendars in their own SQL.
--   2. Date is the one dimension that does *not* get a hashed surrogate key.
--      yyyyMMdd is used instead: it is sortable, human-readable in the fact
--      table, and lets you partition by it later.
--
-- `sequence()` is native Spark SQL and does the same job as
-- dbt_utils.date_spine with a fraction of the compiled SQL.

with spine as (

    select explode(
        sequence(to_date('1996-01-01'), to_date('1999-12-31'), interval 1 day)
    ) as date_day

),

final as (

    select
        cast(date_format(date_day, 'yyyyMMdd') as int)   as date_key,
        date_day,

        year(date_day)                                   as year_number,
        quarter(date_day)                                as quarter_number,
        month(date_day)                                  as month_number,
        date_format(date_day, 'MMMM')                    as month_name,
        date_format(date_day, 'yyyy-MM')                 as year_month,
        concat(year(date_day), '-Q', quarter(date_day))  as year_quarter,
        day(date_day)                                    as day_of_month,
        dayofweek(date_day)                              as day_of_week,
        date_format(date_day, 'EEEE')                    as day_name,
        weekofyear(date_day)                             as week_of_year,

        case
            when dayofweek(date_day) in (1, 7) then true
            else false
        end                                              as is_weekend

    from spine

)

select * from final
