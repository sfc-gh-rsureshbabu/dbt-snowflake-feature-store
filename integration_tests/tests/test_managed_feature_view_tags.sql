-- Test that managed (Dynamic Table) feature views have required TAGs

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = fs_config.get('database') %}
{% set fs_schema = fs_config.get('schema') %}

WITH feature_view_tags AS (
  SELECT 
    object_name,
    tag_name,
    tag_value
  FROM TABLE({{ fs_database }}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    '{{ fs_database }}.{{ fs_schema }}.TEST_MANAGED_CUSTOMER_FEATURES$1.0',
    'DYNAMIC_TABLE'
  ))
  WHERE tag_name IN (
    'SNOWML_FEATURE_STORE_OBJECT',
    'SNOWML_FEATURE_VIEW_METADATA'
  )
),

-- Validate tag values
object_tag AS (
  SELECT 
    PARSE_JSON(tag_value) AS metadata
  FROM feature_view_tags
  WHERE tag_name = 'SNOWML_FEATURE_STORE_OBJECT'
)

-- Test should fail if:
-- 1. Required tags are missing
-- 2. Type is not MANAGED_FEATURE_VIEW
SELECT 
  'Missing required tags or incorrect type' AS error
FROM feature_view_tags
CROSS JOIN object_tag
HAVING 
  COUNT(DISTINCT tag_name) < 2
  OR MAX(object_tag.metadata:type::STRING) != 'MANAGED_FEATURE_VIEW'


