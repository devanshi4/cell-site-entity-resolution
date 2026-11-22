-- A SINGULAR test: a one-off SQL file in tests/. Fails if it returns any rows.
-- SCD2 invariant #1: every customer has exactly one open (current) version.
select customer_id, count(*) as current_versions
from {{ ref('dim_customer') }}
where is_current
group by customer_id
having count(*) <> 1
