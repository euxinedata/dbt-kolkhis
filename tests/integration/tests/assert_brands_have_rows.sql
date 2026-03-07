-- Singular test: mat_table_brands must have at least 1 row
SELECT 1
FROM {{ ref('mat_table_brands') }}
HAVING count(*) = 0
