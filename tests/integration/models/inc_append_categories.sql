{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT
    category_id,
    name
FROM {{ source('retail_products', 'categories') }}
WHERE category_id <= 5

{% if is_incremental() %}
    AND category_id > (SELECT coalesce(max(category_id), 0) FROM {{ this }})
{% endif %}
