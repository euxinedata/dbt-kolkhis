{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['category_id', 'name']
) }}

SELECT
    category_id,
    name
FROM {{ source('retail_products', 'categories') }}
WHERE category_id <= 5
