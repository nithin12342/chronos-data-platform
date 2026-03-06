{{
  config(
    materialized='incremental',
    unique_key='customer_id'
  )
}}

WITH source_customers AS (
    SELECT
        customer_id,
        name,
        email,
        created_at,
        COALESCE(segment, 'standard') AS customer_segment,
        COALESCE(region, 'unknown') AS region,
        updated_at
    FROM {{ source('bronze', 'customers') }}
),

customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(amount) AS lifetime_value,
        MAX(order_date) AS last_order_date
    FROM {{ ref('fct_orders') }}
    GROUP BY customer_id
),

enriched_customers AS (
    SELECT
        sc.*,
        COALESCE(co.total_orders, 0) AS order_count,
        COALESCE(co.lifetime_value, 0) AS lifetime_value,
        co.last_order_date
    FROM source_customers sc
    LEFT JOIN customer_orders co ON sc.customer_id = co.customer_id
)

SELECT
    customer_id,
    name,
    email,
    created_at,
    customer_segment,
    region,
    order_count,
    lifetime_value,
    last_order_date,
    CASE
        WHEN lifetime_value > 10000 THEN 'platinum'
        WHEN lifetime_value > 5000 THEN 'gold'
        WHEN lifetime_value > 1000 THEN 'silver'
        ELSE 'bronze'
    END AS tier,
    CURRENT_TIMESTAMP() AS enriched_at
FROM enriched_customers
{% if is_incremental() %}
WHERE enriched_at > (SELECT MAX(enriched_at) FROM {{ this }})
{% endif %}
