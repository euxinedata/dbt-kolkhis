{{ config(materialized='table') }}

SELECT
    brand_id,
    brand_name_upper
FROM {{ ref('eph_brand_names') }}
