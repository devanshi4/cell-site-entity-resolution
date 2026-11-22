select
    site_id,
    trim(site_name)       as site_name,
    trim(city)            as city,
    upper(technology)     as technology,
    latitude,
    longitude
from {{ source('telecom', 'cell_sites') }}
