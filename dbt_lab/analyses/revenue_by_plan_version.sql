-- ANALYSES: SQL that dbt compiles (ref() works) but never runs or materializes.
-- Use for ad-hoc questions, one-off audits, or queries you hand to an analyst.
-- `dbt compile` then run target/compiled/telecom_dbt/analyses/revenue_by_plan_version.sql
--
-- Question from Finance: "How much did Priya (C002) pay on each plan she has been on?"
select
    c.customer_id,
    c.full_name,
    c.plan_code,
    c.valid_from,
    c.valid_to,
    count(f.event_id)        as events,
    sum(f.usage_charge)      as revenue
from {{ ref('dim_customer') }} c
left join {{ ref('fct_calls') }} f on f.customer_sk = c.customer_sk
where c.customer_id = 'C002'
group by all
order by c.valid_from
