-- DIMENSION from a SEED: ref() works on seeds exactly like on models.
select
    plan_code,
    plan_name,
    monthly_fee,
    per_minute_rate,
    per_mb_rate,
    per_sms_rate,
    per_minute_rate = 0 and per_mb_rate = 0 as is_unlimited
from {{ ref('plan_catalog') }}
