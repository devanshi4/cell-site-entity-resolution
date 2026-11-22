-- DATE DIMENSION: generated, not loaded. Lets BI slice by weekday/month without date math.
-- var() reads from dbt_project.yml (or --vars on the command line).
with spine as (
    select cast(d as date) as date_day
    from range(
        cast('{{ var("date_spine_start") }}' as date),
        cast('{{ var("date_spine_end") }}' as date) + interval 1 day,
        interval 1 day
    ) t(d)
)

select
    date_day,
    extract(year from date_day)          as year,
    extract(month from date_day)         as month,
    strftime(date_day, '%B')             as month_name,
    extract(isodow from date_day)        as day_of_week,      -- 1 = Monday
    strftime(date_day, '%A')             as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from spine
