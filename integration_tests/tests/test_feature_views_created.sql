-- Test that feature views are created with correct naming pattern
-- Feature views should have name$version pattern

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = fs_config.get('database') %}
{% set fs_schema = fs_config.get('schema') %}

SELECT 
  table_name
FROM {{ fs_database }}.INFORMATION_SCHEMA.VIEWS
WHERE table_schema = '{{ fs_schema }}'
  AND table_name LIKE '%$%'  -- Should match FEATURE_NAME$VERSION pattern
HAVING COUNT(*) = 0  -- Should fail if no feature views found


