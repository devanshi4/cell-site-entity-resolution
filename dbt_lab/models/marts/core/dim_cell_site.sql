-- DIMENSION (Type 1): we only care about the current attributes; overwrite on change.
select
    {{ surrogate_key(['site_id']) }} as cell_site_sk,
    site_id,
    site_name,
    city,
    technology,
    latitude,
    longitude
from {{ ref('stg_telecom__cell_sites') }}
