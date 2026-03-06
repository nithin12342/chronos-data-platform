{{
  config(
    materialized='incremental',
    unique_key='order_id'
  )
}}

WITH source_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        created_at,
        updated_at
    FROM {{ source('bronze', 'orders') }}
),

order_items AS (
    SELECT
        order_id,
        COUNT(*) AS items_count,
        SUM(quantity) AS total_quantity,
        SUM(price * quantity) AS items_total
    FROM {{ source('bronze', 'order_items') }}
    GROUP BY order_id
),

enriched_orders AS (
    SELECT
        o.*,
        COALESCE(oi.items_count, 0) AS items_count,
        COALESCE(oi.total_quantity, 0) AS total_quantity,
        COALESCE(oi.items_total, 0) AS items_total,
        CASE 
            WHEN o.total_amount != oi.items_total THEN 'amount_mismatch'
            ELSE 'valid'
        END AS validation_status
    FROM source_orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
)

SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    items_count,
    total_quantity,
    items_total,
    validation_status,
    CURRENT_TIMESTAMP() AS processed_at
FROM enriched_orders
{% if is_incremental() %}
WHERE processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}
