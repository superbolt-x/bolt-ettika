-- The la_* aliases must equal the original linear-all columns.
select *
from {{ ref('blended_performance') }}
where abs(la_orders - purchases) > 0.0001
   or abs(la_new_orders - new_purchases) > 0.0001
   or abs(la_revenue - revenue) > 0.01
   or abs(la_net_revenue - net_revenue) > 0.01
   or abs(la_net_new_revenue - net_new_revenue) > 0.01
