{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH refund_order_data AS (

    -- Sales rows
    SELECT 
        date,
        order_id,
        customer_order_index,
		1 AS order_count,
        gross_revenue,
        total_revenue,
        subtotal_discount,
        shipping_price,
        total_tax,
        shipping_discount,
        0 AS subtotal_refund,
        0 AS shipping_refund,
        0 AS tax_refund
    FROM reporting.ettika_shopify_daily_sales_by_order

    UNION ALL

    -- Refund rows
    SELECT 
        date,
        order_id,
        customer_order_index,
        0,0,0,0,0,0,0,
        subtotal_refund,
        shipping_refund,
        tax_refund
    FROM reporting.ettika_shopify_daily_refunds

)

, tw_data AS (

    SELECT 
        order_id,
		order_date,
		click_date,

        CASE 
            WHEN source = 'facebook-ads' AND campaign_id != '' THEN 'Facebook'
            WHEN source = 'applovin' AND campaign_id != '' THEN 'App Lovin'
            WHEN source = 'google-ads' THEN 'Google Ads'
			WHEN source = 'tiktok-ads' THEN 'Tiktok'
            ELSE 'Other'
        END AS channel,

        CASE 
            WHEN source IN ('facebook-ads','google-ads','applovin')
                 AND campaign_id != ''
            THEN campaign_id
            ELSE '(not set)'
        END AS campaign_id,
		CASE 
            WHEN source IN ('facebook-ads','google-ads')
                 AND adset_id != ''
            THEN adset_id
            ELSE '(not set)'
        END AS adset_id,
		CASE 
            WHEN source IN ('facebook-ads','google-ads')
                 AND ad_id != ''
            THEN ad_id
            ELSE '(not set)'
        END AS ad_id,
		index,
		count(*) over (partition by order_id) as order_index

    FROM triplewhale_raw.orders_attribution
	LEFT JOIN (select order_id, order_date FROM reporting.ettika_shopify_daily_sales_by_order) 
	USING(order_id)
    WHERE "_fivetran_deleted" = FALSE
      AND type = 'ORDERS_LINEAR_ALL'
	  and click_date >= order_date - 28
	order by order_id desc, index asc

)

, attributed_data AS (

    SELECT
        r.date,
        COALESCE(t.channel,'Other') AS channel,
        COALESCE(t.campaign_id,'(not set)') AS campaign_id,
		COALESCE(t.adset_id,'(not set)') AS adset_id,
		COALESCE(t.ad_id,'(not set)') AS ad_id,

        r.order_id,
        r.customer_order_index,
        COALESCE(t.order_index,1) AS order_index,
		
		-- Order amount 
		r.order_count,

        -- Sales amount
        (
            COALESCE(r.gross_revenue,0)
            - COALESCE(r.subtotal_discount,0)
            + COALESCE(r.total_tax,0)
            + COALESCE(r.shipping_price,0)
            - COALESCE(r.shipping_discount,0)
        ) AS sales_amount,

        -- Refund amount
        (
            COALESCE(r.subtotal_refund,0)
            - COALESCE(r.shipping_refund,0)
            + COALESCE(r.tax_refund,0)
        ) AS refund_amount

    FROM refund_order_data r
    LEFT JOIN tw_data t USING(order_id)

)

, final_sho_data AS (

    SELECT
        date,
        channel,
        campaign_id,
		adset_id,
		ad_id,
        SUM(
            sales_amount::float / order_index::float
        ) AS revenue,

        SUM(
            CASE 
                WHEN customer_order_index = 1
                THEN sales_amount::float / order_index::float
                ELSE 0
            END
        ) AS new_revenue,
        SUM(
            refund_amount::float / order_index::float
        ) AS refunds,

        SUM(
            CASE 
                WHEN customer_order_index = 1
                THEN refund_amount::float / order_index::float
                ELSE 0
            END
        ) AS new_refunds,
        SUM(
            (sales_amount - refund_amount)::float
            / order_index::float
        ) AS net_revenue,
        SUM(order_count::float / order_index::float) AS purchases,

        SUM(
            CASE 
                WHEN customer_order_index = 1
                THEN order_count::float / order_index::float
                ELSE 0
            END
        ) AS new_purchases

    FROM attributed_data
    GROUP BY 1,2,3,4,5

)

, paid_objects as (
	SELECT 'Facebook' as channel, campaign_id::varchar as campaign_id, campaign_name, 
	adset_id::varchar as adset_id, adset_name, ad_id::varchar as ad_id, ad_name, count(*) as nb
	FROM reporting.ettika_facebook_ad_performance 
	group by 1,2,3,4,5,6,7
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
	'(not set)' as adset_id, '(not set)' as adset_name, '(not set)' as ad_id, '(not set)' as ad_name, count(*) as nb
	FROM reporting.ettika_googleads_campaign_performance
	WHERE campaign_name ~* 'Pmax'
	group by 1,2,3,4,5,6,7
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name, 
	ad_group_id::varchar as adset_id, ad_group_name, ad_id::varchar as ad_id, '(not set)' as ad_name, count(*) as nb
	FROM reporting.ettika_googleads_ad_performance
	group by 1,2,3,4,5,6,7
	union all
	SELECT 'App Lovin' as channel, campaign_id_external::varchar as campaign_id, campaign as campaign_name,
	'(not set)' as adset_id, '(not set)' as adset_name, '(not set)' as ad_id, '(not set)' as ad_name, count(*) as nb
	FROM applovin_raw.advertiser_report
	WHERE "_fivetran_deleted" = FALSE
	group by 1,2,3,4,5,6,7
)

, conversion_data as (
	select * from final_sho_data left join paid_objects using(channel,campaign_id,adset_id,ad_id)
)

, paid_data as (
	SELECT 'Facebook' as channel, campaign_id::varchar as campaign_id, campaign_name,
	adset_id::varchar as adset_id, adset_name, ad_id::varchar as ad_id, ad_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(link_clicks) as clicks
	FROM reporting.ettika_facebook_ad_performance 
	where date_granularity = 'day'
	and (spend > 0 or link_clicks > 0 or impressions > 0)
	group by 1,2,3,4,5,6,7,8
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
	'(not set)' as adset_id, '(not set)' as adset_name, '(not set)' as ad_id, '(not set)' as ad_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks
	FROM reporting.ettika_googleads_campaign_performance
	where date_granularity = 'day'
	and (spend > 0 or clicks > 0 or impressions > 0)
	and campaign_name ~* 'Pmax'
	group by 1,2,3,4,5,6,7,8
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
	ad_group_id::varchar as adset_id, ad_group_name, ad_id::varchar as ad_id, '(not set)' as ad_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks
	FROM reporting.ettika_googleads_ad_performance
	where date_granularity = 'day'
	and (spend > 0 or clicks > 0 or impressions > 0)
	group by 1,2,3,4,5,6,7,8
	union all
	SELECT 'App Lovin' as channel, campaign_id_external::varchar as campaign_id, campaign as campaign_name,
	'(not set)' as adset_id, '(not set)' as adset_name, '(not set)' as ad_id, '(not set)' as ad_name,
	date_trunc('day',"day") as date, SUM(CAST(REPLACE(REPLACE(cost, '$', ''), ',', '') AS FLOAT)) as spend, 
	sum(impressions) as impressions, sum(clicks) as clicks
	FROM applovin_raw.advertiser_report
	WHERE "_fivetran_deleted" = FALSE
	group by 1,2,3,4,5,6,7,8
)

, final_data AS (
SELECT 
	date::date as date, channel, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
	coalesce(spend,0) as spend, coalesce(impressions,0) as impressions, coalesce(clicks,0) as clicks, 
	coalesce(net_revenue,0) as net_revenue, coalesce(revenue,0) as revenue, coalesce(new_revenue,0)-coalesce(new_refunds,0) as net_new_revenue, coalesce(new_revenue,0) as new_revenue, coalesce(purchases,0) as purchases, coalesce(new_purchases,0) as new_purchases
FROM conversion_data 
FULL OUTER JOIN paid_data USING(date,channel,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name)
ORDER BY date desc )

select *
from final_data
