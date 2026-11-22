-- Staging on top of the SNAPSHOT (not the raw table), so we clean every historical version.
-- ref() works for snapshots, seeds and models alike: it resolves the table name AND adds a DAG edge,
-- which is how dbt knows to run snap_customers BEFORE this model.

select
    customer_id,
    trim(full_name)                 as full_name,
    lower(trim(email))              as email,
    trim(city)                      as city,
    upper(trim(plan_code))          as plan_code,
    lower(status)                   as status,
    updated_at,
    dbt_scd_id,                     -- dbt's unique id for this version
    dbt_valid_from                  as valid_from,
    dbt_valid_to                    as valid_to
from {{ ref('snap_customers') }}
