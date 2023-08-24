with
    return_policy as (
        {%- set return_policy_query -%}
    CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
    RETURNS Array<String>
    LANGUAGE js AS """
    return Object.keys(JSON.parse(input));
    """;
    WITH keys AS (
    SELECT
        jsonObjectKeys(return_policy) AS keys
    FROM
        {{ref('stg__returns')}}
    WHERE return_policy IS NOT NULL
    )
    SELECT
    DISTINCT k
    FROM keys
    CROSS JOIN UNNEST(keys.keys) AS k
        {%- endset -%}

        {%- set results = run_query(return_policy_query) -%}
        {%- if execute -%} {%- set results_list = results.columns[0].values() -%}
        {%- else -%} {%- set results_list = [] -%}
        {%- endif -%}

        select
            return_id,
            order_id,
            {% for key in results_list -%}
                trim(json_query(return_policy, '$.{{ key }}'), '"') as rp_{{ key }},
            {% endfor -%}

        from {{ ref("stg__returns") }}
        where return_policy is not null
    ),

shop_return_policy AS (

    SELECT 
        CAST(rp_shop_id AS INT) AS shop_id,
        -- This could be done with jinja for loop!!!
        ARRAY_AGG(rp_instant_exchange_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS instant_exchange_enabled,
        ARRAY_AGG(rp_gift_cards_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS gift_cards_enabled,
        ARRAY_AGG(rp_shop_later_gift_card_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS shop_later_gift_card_enabled,
        ARRAY_AGG(rp_refunds_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS refunds_enabled,
        ARRAY_AGG(rp_keep_item_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS keep_item_enabled,
        ARRAY_AGG(rp_keep_item_threshold ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS keep_item_threshold,
        ARRAY_AGG(rp_persistent_credit_gift_card_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS persistent_credit_gift_card_enabled,
        ARRAY_AGG(rp_persistent_credit_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS persistent_credit_enabled,
        ARRAY_AGG(rp_persistent_credit_exchanges_enabled ORDER BY rp_created_at DESC LIMIT 1)[OFFSET(0)] AS persistent_credit_exchanges_enabled
    FROM return_policy
    WHERE rp_persistent_credit_enabled IS NOT NULL --AND rp_keep_item_threshold IN ('true', 'false')
    GROUP BY 1
    )


SELECT
    orders.order_id,
    orders.order_created_at,
    orders.order_processed_at,
    orders.line_item_count,
    returns.item_count AS return_item_count,
    returns.return_id,
    returns.return_processed_at,
    orders.shop_id,
    orders.gross_merchandise_value_usd AS gmv_usd,
    merchants.benchmark_vertical AS merchant_product_category,
    instant_exchange_enabled,
    gift_cards_enabled,
    shop_later_gift_card_enabled,
    refunds_enabled,
    keep_item_enabled,
    keep_item_threshold,
    CASE WHEN keep_item_threshold != '0' OR keep_item_threshold IS NOT NULL THEN TRUE ELSE FALSE END AS has_keep_item_threshold,
    -- ASSUMING persistent credit enabled means true in any form (since its all false for persistent_credit_enabled)
    -- Logic below assumes null is false
    CASE WHEN persistent_credit_gift_card_enabled IN ('true','1') THEN TRUE ELSE FALSE END AS persistent_credit_gift_card_enabled,
    CASE WHEN persistent_credit_enabled IN ('true','1') THEN TRUE ELSE FALSE END AS persistent_credit_enabled,
    CASE WHEN persistent_credit_exchanges_enabled IN ('true','1') THEN TRUE ELSE FALSE END AS persistent_credit_exchanges_enabled
FROM 
    {{ ref('stg__orders') }} AS orders
    -- ASSUMING THE RETURN POLICY IS SHOP SPECIFIC AND NOT ORDER SPECIFIC Hence we took the latest values
    LEFT JOIN shop_return_policy AS srp USING (shop_id)
    LEFT JOIN {{ ref('stg__merchants') }} AS merchants USING(shop_id)
    -- This join won't return anything for given data but this logic stands otherwise
    LEFT JOIN {{ ref('stg__returns') }} AS returns USING (order_id)

