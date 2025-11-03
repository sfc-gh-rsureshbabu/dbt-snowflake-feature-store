-- Test that Dynamic Table feature views are created correctly

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = fs_config.get('database') %}
{% set fs_schema = fs_config.get('schema') %}

-- Check that the managed feature view exists as a DYNAMIC TABLE
SELECT 
  'TEST_MANAGED_CUSTOMER_FEATURES$1.0' AS expected_name,
  'DYNAMIC_TABLE' AS expected_type
QUALIFY NOT EXISTS (
  SELECT 1
  FROM {{ fs_database }}.INFORMATION_SCHEMA.DYNAMIC_TABLES
  WHERE table_schema = '{{ fs_schema | upper }}'
    AND table_name = 'TEST_MANAGED_CUSTOMER_FEATURES$1.0'
)


