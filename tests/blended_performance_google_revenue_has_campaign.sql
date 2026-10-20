{{ config(severity = 'warn') }}
-- Google revenue should carry a campaign name. Free PMax listing clicks (ad_id 'sag_organic') used to strand about a
-- third of Google revenue with no name; they are folded into '(not set)' now. Remaining rows are older campaigns that
-- are not in the Google ad performance tables. Warn only.
select date, campaign_id, sum(net_revenue) as net_revenue, sum(ta28_revenue) as ta28_revenue
from {{ ref('blended_performance') }}
where channel = 'Google Ads'
  and campaign_name is null
  and date >= current_date - 90
  and (net_revenue > 0 or ta28_revenue > 0)
group by 1, 2
