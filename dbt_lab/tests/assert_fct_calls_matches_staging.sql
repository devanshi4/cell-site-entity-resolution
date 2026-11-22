-- Incremental models can silently drift from the source (missed late rows, bad filter).
-- Reconciliation test: every staged event must exist in the fact exactly once.
select s.event_id
from {{ ref('stg_telecom__call_events') }} s
left join {{ ref('fct_calls') }} f using (event_id)
where f.event_id is null
