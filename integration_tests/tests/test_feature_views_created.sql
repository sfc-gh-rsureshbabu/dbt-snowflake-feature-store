-- Test that feature views are created with correct naming pattern
-- Feature views should have name$version pattern

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = target.database %}
{% set fs_schema = fs_config.get('schema') %}

-- Test should fail if no feature views exist
WITH feature_views AS (
  SELECT COUNT(*) AS fv_count
  FROM INFORMATION_SCHEMA.VIEWS
  WHERE table_schema = '{{ fs_schema }}'
    AND table_name LIKE '%$%'  -- Should match FEATURE_NAME$VERSION pattern
)
SELECT 
  'No feature views found' AS error
FROM feature_views
WHERE fv_count = 0


