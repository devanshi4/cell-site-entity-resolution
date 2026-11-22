-- REPORTING model: pre-aggregated to exactly what the dashboard shows.
-- BI tools hit this small table instead of scanning billions of CDRs every page load.
-- Plain table (full rebuild): cheap because it reads the already-clean fact.
select
    f.event_date,
    d.day_name,
    d.is_weekend,
    f.customer_city_at_event        as city,
    f.plan_code,
    count(*)                        as events,
    count(distinct f.customer_id)   as active_customers,
    sum(f.billable_minutes)         as billable_minutes,
    sum(f.data_mb)                  as data_mb,
    sum(f.usage_charge)             as usage_revenue
from {{ ref('fct_calls') }} f
join {{ ref('dim_date') }} d on f.event_date = d.date_day
group by all
