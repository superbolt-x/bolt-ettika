{{ config (
    alias = target.database + '_blended_performance'
)}}

{#- Triple Whale touchpoint -> reporting keys. Shared by the linear-all (tw_data) and triple attribution (tw_triple) CTEs.
    Free PMax listing clicks come through with ad_id 'sag_organic'; the spend rows of PMax campaigns carry '(not set)',
    so 'sag_organic' is folded into '(not set)' to let the revenue join its campaign. -#}
{% set tw_channel %}
        CASE
            WHEN source = 'facebook-ads' AND campaign_id != '' THEN 'Facebook'
            WHEN source = 'applovin' AND campaign_id != '' THEN 'App Lovin'
            WHEN source = 'google-ads' THEN 'Google Ads'
            WHEN source = 'tiktok-ads' THEN 'Tiktok'
            WHEN source = 'pinterest-ads' THEN 'Pinterest'
            ELSE 'Other'
        END
{% endset %}
{% set tw_campaign_id %}
        CASE
            WHEN source IN ('facebook-ads','google-ads','applovin')
                 AND campaign_id != ''
            THEN campaign_id
            ELSE '(not set)'
        END
{% endset %}
{% set tw_adset_id %}
        CASE
            WHEN source IN ('facebook-ads','google-ads')
                 AND adset_id != ''
            THEN adset_id
            ELSE '(not set)'
        END
{% endset %}
{% set tw_ad_id %}
        CASE
            WHEN source IN ('facebook-ads','google-ads')
                 AND ad_id != ''
                 AND ad_id != 'sag_organic'
            THEN ad_id
            ELSE '(not set)'
        END
{% endset %}

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

    -- Linear-all view (existing columns, aliased la_*): touchpoints within 28 days before the order,
    -- revenue and orders split evenly across the touchpoints
    SELECT 
        order_id,
		date as order_date,
		click_date,
        {{ tw_channel }} AS channel,
        {{ tw_campaign_id }} AS campaign_id,
		{{ tw_adset_id }} AS adset_id,
		{{ tw_ad_id }} AS ad_id,
		index,
		count(*) over (partition by order_id) as order_index

    FROM triplewhale_raw.orders_attribution
	LEFT JOIN (select order_id, date FROM reporting.ettika_shopify_daily_sales_by_order) 
	USING(order_id)
    WHERE "_fivetran_deleted" = FALSE
      AND type = 'ORDERS_LINEAR_ALL'
	  and click_date >= order_date - 28
	order by order_id desc, index asc

)

, tw_triple AS (

    -- Triple attribution view (ta28_*, talt_*): 100% credit to the last ad click of each channel.
    -- ORDERS_LAST_PLATFORM_CLICK holds one touchpoint per platform per order; keep the latest per channel.
    SELECT
        a.order_id,
        o.created_at,
        a.click_date,
        {{ tw_channel }} AS channel,
        {{ tw_campaign_id }} AS campaign_id,
        {{ tw_adset_id }} AS adset_id,
        {{ tw_ad_id }} AS ad_id,
        ROW_NUMBER() OVER (
            PARTITION BY a.order_id, {{ tw_channel }}
            ORDER BY a.click_date DESC
        ) AS rn

    FROM triplewhale_raw.orders_attribution a
    JOIN triplewhale_raw.orders o
        ON o.order_id = a.order_id
    WHERE COALESCE(a."_fivetran_deleted", FALSE) = FALSE
      AND COALESCE(o."_fivetran_deleted", FALSE) = FALSE
      AND a.type = 'ORDERS_LAST_PLATFORM_CLICK'

)

, triple_attributed AS (

    SELECT
        r.date,
        t.channel,
        t.campaign_id,
        t.adset_id,
        t.ad_id,
        r.order_count,
        r.customer_order_index,
        -- 28-day window on the order timestamp, as in the Triple Whale attribution page
        (t.click_date >= t.created_at - INTERVAL '28 day') AS in_28d,
        (
            COALESCE(r.gross_revenue,0)
            - COALESCE(r.subtotal_discount,0)
            + COALESCE(r.total_tax,0)
            + COALESCE(r.shipping_price,0)
            - COALESCE(r.shipping_discount,0)
        ) AS sales_amount,
        (
            COALESCE(r.subtotal_refund,0)
            - COALESCE(r.shipping_refund,0)
            + COALESCE(r.tax_refund,0)
        ) AS refund_amount

    FROM refund_order_data r
    JOIN tw_triple t
        ON t.order_id = r.order_id
       AND t.rn = 1

)

, triple_data AS (

    -- Full credit per channel: an order can sit in several channels, so never sum these columns across channels.
    -- New customer = customer_order_index 1, the same rule as new_purchases / new_revenue.
    SELECT
        date,
        channel,
        campaign_id,
        adset_id,
        ad_id,

        SUM(CASE WHEN in_28d THEN order_count ELSE 0 END) AS ta28_orders,
        SUM(CASE WHEN in_28d AND customer_order_index = 1 THEN order_count ELSE 0 END) AS ta28_new_orders,
        SUM(CASE WHEN in_28d THEN sales_amount ELSE 0 END) AS ta28_revenue,
        SUM(CASE WHEN in_28d AND customer_order_index = 1 THEN sales_amount ELSE 0 END) AS ta28_new_revenue,
        SUM(CASE WHEN in_28d THEN sales_amount - refund_amount ELSE 0 END) AS ta28_net_revenue,
        SUM(CASE WHEN in_28d AND customer_order_index = 1 THEN sales_amount - refund_amount ELSE 0 END) AS ta28_net_new_revenue,

        SUM(order_count) AS talt_orders,
        SUM(CASE WHEN customer_order_index = 1 THEN order_count ELSE 0 END) AS talt_new_orders,
        SUM(sales_amount) AS talt_revenue,
        SUM(CASE WHEN customer_order_index = 1 THEN sales_amount ELSE 0 END) AS talt_new_revenue,
        SUM(sales_amount - refund_amount) AS talt_net_revenue,
        SUM(CASE WHEN customer_order_index = 1 THEN sales_amount - refund_amount ELSE 0 END) AS talt_net_new_revenue

    FROM triple_attributed
    GROUP BY 1,2,3,4,5

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

, final_sho_data_all AS (
	SELECT * FROM final_sho_data
	FULL OUTER JOIN triple_data USING(date,channel,campaign_id,adset_id,ad_id)
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
	union all
	SELECT 'Pinterest' as channel, '(not set)' as campaign_id, '(not set)' as campaign_name,
	'(not set)' as adset_id, '(not set)' as adset_name, '(not set)' as ad_id, '(not set)' as ad_name, count(*) as nb
	FROM reporting.ettika_pinterest_ad_group_performance
	group by 1,2,3,4,5,6,7
)

, conversion_data as (
	select * from final_sho_data_all left join paid_objects using(channel,campaign_id,adset_id,ad_id)
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
	union all
	SELECT 'Pinterest' as channel, '(not set)' as campaign_id, '(not set)' as campaign_name,
	'(not set)' as adset_id, '(not set)' as adset_name, '(not set)' as ad_id, '(not set)' as ad_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks
	FROM reporting.ettika_pinterest_ad_group_performance 
	where date_granularity = 'day'
	and (spend > 0 or clicks > 0 or impressions > 0)
	group by 1,2,3,4,5,6,7,8
)

, final_data AS (
SELECT 
	date::date as date, channel, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
	coalesce(spend,0) as spend, coalesce(impressions,0) as impressions, coalesce(clicks,0) as clicks, 
	coalesce(net_revenue,0) as net_revenue, coalesce(revenue,0) as revenue, coalesce(new_revenue,0)-coalesce(new_refunds,0) as net_new_revenue, coalesce(new_revenue,0) as new_revenue, coalesce(purchases,0) as purchases, coalesce(new_purchases,0) as new_purchases,
	-- same linear-all values under explicit names
	coalesce(purchases,0) as la_orders, coalesce(new_purchases,0) as la_new_orders,
	coalesce(revenue,0) as la_revenue, coalesce(new_revenue,0) as la_new_revenue,
	coalesce(net_revenue,0) as la_net_revenue, coalesce(new_revenue,0)-coalesce(new_refunds,0) as la_net_new_revenue,
	-- triple attribution, 28-day window and lifetime
	coalesce(ta28_orders,0) as ta28_orders, coalesce(ta28_new_orders,0) as ta28_new_orders,
	coalesce(ta28_revenue,0) as ta28_revenue, coalesce(ta28_new_revenue,0) as ta28_new_revenue,
	coalesce(ta28_net_revenue,0) as ta28_net_revenue, coalesce(ta28_net_new_revenue,0) as ta28_net_new_revenue,
	coalesce(talt_orders,0) as talt_orders, coalesce(talt_new_orders,0) as talt_new_orders,
	coalesce(talt_revenue,0) as talt_revenue, coalesce(talt_new_revenue,0) as talt_new_revenue,
	coalesce(talt_net_revenue,0) as talt_net_revenue, coalesce(talt_net_new_revenue,0) as talt_net_new_revenue
FROM conversion_data 
FULL OUTER JOIN paid_data USING(date,channel,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name)
ORDER BY date desc )

select *
from final_data
