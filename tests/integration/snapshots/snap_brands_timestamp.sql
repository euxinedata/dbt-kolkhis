{% snapshot snap_brands_timestamp %}

{{ config(
    target_schema='dbt_petkov_venelin',
    unique_key='brand_id',
    strategy='timestamp',
    updated_at='updated_at',
) }}

SELECT
    brand_id,
    name,
    now() AS updated_at
FROM {{ source('retail_products', 'brands') }}
WHERE brand_id <= 5

{% endsnapshot %}
