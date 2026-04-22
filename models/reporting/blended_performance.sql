{{ config (
    alias = target.database + '_blended_performance'
)}}

with
tw_data AS (

    SELECT 
        order_id,
		order_date,
		click_date,

        CASE 
            WHEN source = 'facebook-ads' AND campaign_id != '' THEN 'Facebook'
            WHEN source = 'applovin' AND campaign_id != '' THEN 'App Lovin'
            WHEN source = 'google-ads' THEN 'Google Ads'
            ELSE 'Other'
        END AS channel,

        CASE 
            WHEN source IN ('facebook-ads','google-ads','applovin')
                 AND campaign_id != ''
            THEN campaign_id
            ELSE '(not set)'
        END AS campaign_id,
		index,
		count(*) over (partition by order_id) as order_index

    FROM triplewhale_raw.orders_attribution
	LEFT JOIN orders using(order_id)
    WHERE "_fivetran_deleted" = FALSE
      AND type = 'ORDERS_LINEAR_ALL'
	  and click_date::date >= order_date::date - 28
	order by order_id desc, index asc

)
	

, attributed_data AS (

    SELECT
        r.date,
        COALESCE(t.channel,'Other') AS channel,
        COALESCE(t.campaign_id,'(not set)') AS campaign_id,

        COALESCE(t.order_index,1) AS order_index,
		
		-- Order amount 
		s.orders,
		s.first_orders,

        -- Sales amount
        (
            COALESCE(s.gross_sales,0)
            - COALESCE(s.subtotal_discounts,0)
            + COALESCE(s.tax_sales,0)
            + COALESCE(s.shipping_revenue,0)
            - COALESCE(s.shipping_discounts,0)
        ) AS sales_amount,
		(
				COALESCE(s.first_order_gross_sales,0)
				- COALESCE(s.first_order_subtotal_discounts,0)
				+ COALESCE(s.first_order_tax_sales,0)
				+ COALESCE(s.first_order_shipping_revenue,0)
				- COALESCE(s.first_order_shipping_discounts,0)
		) AS first_order_sales_amount,

        -- Refund amount
        (
            COALESCE(s.subtotal_refunds,0)
            - COALESCE(s.shipping_refunds,0)
            + COALESCE(s.tax_refunds,0)
        ) AS refund_amount,
		(
	            COALESCE(s.first_order_subtotal_refunds,0)
	            - COALESCE(s.first_order_shipping_refunds,0)
	            + COALESCE(s.first_order_tax_refunds,0)
	    ) AS first_order_refund_amount,

    FROM {{ source('reporting','shopify_sales') }} s
    LEFT JOIN tw_data t USING(order_id)

)

, final_sho_data AS (

    SELECT
        date,
        channel,
        campaign_id,
        SUM(
            sales_amount::float / order_index::float
        ) AS revenue,
        SUM(
            firt_order_sales_amount::float / order_index::float
        ) AS new_revenue,
        SUM(
            refund_amount::float / order_index::float
        ) AS refunds,
        SUM(
            first_order_refund_amount::float / order_index::float
        ) AS new_refunds,
        SUM(
            (sales_amount - refund_amount)::float
            / order_index::float
        ) AS net_revenue,
        SUM(orders::float / order_index::float) AS purchases,
        SUM(first_orders::float / order_index::float) AS new_purchases,

        

    FROM attributed_data
    GROUP BY 1,2,3

)

, paid_objects as (
	SELECT 'Facebook' as channel, campaign_id::varchar as campaign_id, campaign_name, count(*) as nb
	FROM reporting.ettika_facebook_ad_performance 
	group by 1,2,3
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name, count(*) as nb
	FROM reporting.ettika_googleads_campaign_performance
	group by 1,2,3
	union all
	SELECT 'App Lovin' as channel, campaign_id_external::varchar as campaign_id, campaign as campaign_name, count(*) as nb
	FROM applovin_raw.advertiser_report
	WHERE "_fivetran_deleted" = FALSE
	group by 1,2,3
)

, conversion_data as (
	select * from final_sho_data left join paid_objects using(channel,campaign_id)
)

, paid_data as (
	SELECT 'Facebook' as channel, campaign_id::varchar as campaign_id, campaign_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(link_clicks) as clicks
	FROM reporting.ettika_facebook_ad_performance 
	where date_granularity = 'day'
	and (spend > 0 or link_clicks > 0 or impressions > 0)
	group by 1,2,3,4
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks
	FROM reporting.ettika_googleads_campaign_performance
	where date_granularity = 'day'
	and (spend > 0 or clicks > 0 or impressions > 0)
	and campaign_name ~* 'Pmax'
	group by 1,2,3,4
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks
	FROM reporting.ettika_googleads_ad_performance
	where date_granularity = 'day'
	and (spend > 0 or clicks > 0 or impressions > 0)
	group by 1,2,3,4
	union all
	SELECT 'App Lovin' as channel, campaign_id_external::varchar as campaign_id, campaign as campaign_name, 
	date_trunc('day',"day") as date, SUM(CAST(REPLACE(REPLACE(cost, '$', ''), ',', '') AS FLOAT)) as spend, 
	sum(impressions) as impressions, sum(clicks) as clicks
	FROM applovin_raw.advertiser_report
	WHERE "_fivetran_deleted" = FALSE
	group by 1,2,3,4
)

, final_data AS (
SELECT 
	date::date as date, channel, campaign_id, campaign_name,
	coalesce(spend,0) as spend, coalesce(impressions,0) as impressions, coalesce(clicks,0) as clicks, 
	coalesce(net_revenue,0) as net_revenue, coalesce(revenue,0) as revenue, coalesce(new_revenue,0)-coalesce(new_refunds,0) as net_new_revenue, coalesce(new_revenue,0) as new_revenue, coalesce(purchases,0) as purchases, coalesce(new_purchases,0) as new_purchases
FROM conversion_data 
FULL OUTER JOIN paid_data USING(date,channel,campaign_id,campaign_name)
ORDER BY date desc )

select *
from final_data
