{{ config(materialized='table') }}

SELECT
    b.brand_id,
    b.name AS brand_name,
    count(p.product_id) AS product_count
FROM {{ source('retail_products', 'brands') }} b
LEFT JOIN {{ source('retail_products', 'products') }} p
    ON b.brand_id = p.brand_id
WHERE b.brand_id <= 10
GROUP BY b.brand_id, b.name
