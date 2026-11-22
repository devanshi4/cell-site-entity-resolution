-- SCD2 invariant #2: a customer's versions never overlap in time.
-- If they did, a point-in-time join would match two versions and DUPLICATE fact rows.
select a.customer_id, a.customer_sk, b.customer_sk as overlapping_sk
from {{ ref('dim_customer') }} a
join {{ ref('dim_customer') }} b
  on  a.customer_id = b.customer_id
  and a.customer_sk <> b.customer_sk
  and a.valid_from < b.valid_to
  and b.valid_from < a.valid_to
