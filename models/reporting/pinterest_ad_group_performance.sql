{{ config (
    alias = target.database + '_pinterest_ad_group_performance'
)}}

SELECT 
advertiser_name,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
ad_group_status,
audience,
date,
date_granularity,
0 as spend,
impression_2 as impressions,
clickthrough_2 as clicks,
0 as add_to_cart,
0 as purchases,
0 as revenue
FROM {{ ref('pinterest_performance_by_ad_group') }}
