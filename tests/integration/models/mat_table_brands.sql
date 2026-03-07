{{ config(materialized='table') }}

SELECT
    brand_id,
    name
FROM {{ source('retail_products', 'brands') }}
WHERE brand_id <= 10
