{{ config(materialized='view') }}

SELECT count(*) AS total_brands
FROM {{ ref('mat_table_brands') }}
