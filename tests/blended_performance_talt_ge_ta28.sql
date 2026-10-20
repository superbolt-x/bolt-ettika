-- Lifetime triple attribution is a superset of the 28-day window.
select *
from {{ ref('blended_performance') }}
where talt_orders < ta28_orders - 0.001
   or talt_revenue < ta28_revenue - 0.01
