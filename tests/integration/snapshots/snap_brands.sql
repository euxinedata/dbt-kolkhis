{% snapshot snap_brands %}

{{ config(
    target_schema='dbt_petkov_venelin',
    unique_key='brand_id',
    strategy='check',
    check_cols=['name'],
) }}

SELECT
    brand_id,
    name
FROM {{ source('retail_products', 'brands') }}
WHERE brand_id <= 5

{% endsnapshot %}
