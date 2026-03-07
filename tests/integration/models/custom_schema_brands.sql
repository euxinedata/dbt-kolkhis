{{ config(
    materialized='table',
    schema='custom_test'
) }}

SELECT
    brand_id,
    name
FROM {{ source('retail_products', 'brands') }}
WHERE brand_id <= 3
