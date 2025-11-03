-- Test that feature views have required TAGs
-- Each feature view should have SNOWML_FEATURE_STORE_OBJECT and SNOWML_FEATURE_VIEW_METADATA tags

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = fs_config.get('database') %}
{% set fs_schema = fs_config.get('schema') %}

WITH feature_view_tags AS (
  SELECT 
    object_name,
    tag_name,
    tag_value
  FROM TABLE({{ fs_database }}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    '{{ fs_database }}.{{ fs_schema }}.TEST_STATIC_CUSTOMER_FEATURES$1.0',
    'VIEW'
  ))
  WHERE tag_name IN (
    'SNOWML_FEATURE_STORE_OBJECT',
    'SNOWML_FEATURE_VIEW_METADATA'
  )
)

-- Test should fail if required tags are missing
SELECT 
  'Missing required tags' AS error
FROM feature_view_tags
HAVING COUNT(DISTINCT tag_name) < 2


