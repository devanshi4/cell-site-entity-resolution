-- DIMENSION (SCD Type 2): one row per customer per version.
-- Grain: customer_sk. Facts join here "as of" the event time, so history is reported truthfully.

with history as (
    select * from {{ ref('stg_telecom__customers_history') }}
)

select
    {{ surrogate_key(['customer_id', 'valid_from']) }}   as customer_sk,   -- one key per VERSION
    customer_id,                                                        -- natural/business key
    full_name,
    email,
    city,
    plan_code,
    status,
    valid_from,
    valid_to,
    valid_to = cast('9999-12-31 00:00:00' as timestamp)  as is_current
from history
