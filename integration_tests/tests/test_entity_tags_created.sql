-- Test that entity tags are created in the Feature Store schema
-- This test passes if entity tags are found

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = fs_config.get('database') %}
{% set fs_schema = fs_config.get('schema') %}
{% set fs_full_schema = fs_database ~ '.' ~ fs_schema %}

SELECT 
  tag_name
FROM TABLE({{ fs_database }}.INFORMATION_SCHEMA.TAG_REFERENCES(
  '{{ fs_full_schema }}',
  'SCHEMA'
))
WHERE tag_name LIKE 'SNOWML_FEATURE_STORE_ENTITY_%'
HAVING COUNT(*) = 0  -- Should fail if no entities found


