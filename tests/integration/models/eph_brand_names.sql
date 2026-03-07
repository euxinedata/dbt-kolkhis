{{ config(materialized='ephemeral') }}

SELECT
    brand_id,
    UPPER(name) AS brand_name_upper
FROM {{ source('retail_products', 'brands') }}
WHERE brand_id <= 10
