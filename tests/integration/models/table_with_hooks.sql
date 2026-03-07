{{ config(
    materialized='table',
    pre_hook="SELECT 1",
    post_hook="SELECT 1"
) }}

SELECT
    brand_id,
    name
FROM {{ source('retail_products', 'brands') }}
WHERE brand_id <= 3
