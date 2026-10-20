-- New-customer orders can never exceed all orders, in either triple attribution window.
select *
from {{ ref('blended_performance') }}
where ta28_new_orders > ta28_orders + 0.001
   or talt_new_orders > talt_orders + 0.001
