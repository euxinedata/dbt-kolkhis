{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='brand_id'
) }}

SELECT
    brand_id,
    name
FROM {{ source('retail_products', 'brands') }}
WHERE brand_id <= 5
