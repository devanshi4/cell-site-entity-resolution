-- FACT (incremental): one row per call event. Grain: event_id.
--
-- First run (or --full-refresh): dbt runs the whole SELECT and CREATES the table.
-- Later runs: is_incremental() is true, so the WHERE filter keeps only NEW raw batches,
--             and dbt MERGES them into the existing table on unique_key:
--               event_id already there -> UPDATE (corrections)   new event_id -> INSERT

{#- Jinja comment, because a "--" comment INSIDE {{ }} is a syntax error.
    incremental_strategy options: append | merge | delete+insert | insert_overwrite | microbatch
    on_schema_change options:     ignore | fail | append_new_columns | sync_all_columns -#}
{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns'
    )
}}

with events as (

    select * from {{ ref('stg_telecom__call_events') }}

    {% if is_incremental() %}
    -- {{ '{{ this }}' }} = the fact table as it exists RIGHT NOW in the warehouse.
    -- Filter on LOAD time, not event time: a late event (happened yesterday, arrived today)
    -- still has a new _loaded_at, so it is picked up. Filtering on started_at would MISS it.
    where _loaded_at > (select coalesce(max(_loaded_at), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}

),

customers as (
    select * from {{ ref('dim_customer') }}
),

plans as (
    select * from {{ ref('dim_plan') }}
),

joined as (

    select
        e.event_id,
        c.customer_sk,          -- the customer VERSION valid when the call happened
        e.customer_id,
        e.site_id,
        e.event_date,
        e.started_at,
        e.event_type,
        c.plan_code,            -- plan AT CALL TIME, not today's plan
        c.city                    as customer_city_at_event,
        e.duration_sec,
        e.data_mb,
        p.per_minute_rate,
        p.per_mb_rate,
        p.per_sms_rate,
        e._loaded_at
    from events e
    -- POINT-IN-TIME join to the SCD2 dimension. valid_from inclusive, valid_to exclusive.
    left join customers c
        on  e.customer_id = c.customer_id
        and e.started_at >= c.valid_from
        and e.started_at <  c.valid_to
    left join plans p
        on c.plan_code = p.plan_code

)

select
    event_id,
    customer_sk,
    customer_id,
    site_id,
    event_date,
    started_at,
    event_type,
    plan_code,
    customer_city_at_event,
    duration_sec,
    case when event_type = 'VOICE' then {{ billable_minutes('duration_sec') }} else 0 end as billable_minutes,
    data_mb,
    cast(case event_type
        when 'VOICE' then {{ billable_minutes('duration_sec') }} * per_minute_rate
        when 'DATA'  then data_mb * per_mb_rate
        when 'SMS'   then per_sms_rate
    end as decimal(12, 2))                                     as usage_charge,
    _loaded_at
from joined
