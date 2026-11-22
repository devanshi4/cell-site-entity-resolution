-- STAGING model: one per source table. Rename, cast, clean, dedupe. NO joins, NO business logic.
-- Everything downstream reads from here, never from the raw source again.

with source as (

    -- source() compiles to the real table name, e.g. "telecom"."raw"."call_events",
    -- and tells dbt this model depends on that source (so it shows in the DAG).
    select * from {{ source('telecom', 'call_events') }}

),

deduplicated as (

    -- Raw has re-sends (identical rows) and corrections (same event_id, later _loaded_at).
    -- Rule: the LATEST delivery of an event_id wins.
    select *
    from source
    qualify row_number() over (partition by event_id order by _loaded_at desc) = 1

)

select
    event_id,
    customer_id,
    site_id,
    upper(event_type)                as event_type,
    started_at,
    cast(started_at as date)         as event_date,
    coalesce(duration_sec, 0)        as duration_sec,
    coalesce(data_mb, 0)             as data_mb,
    _loaded_at
from deduplicated
