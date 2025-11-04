-- Test that managed (Dynamic Table) feature views are created successfully
-- Tag validation is done in Python tests due to ACCOUNT_USAGE latency

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = target.database %}
{% set fs_schema = fs_config.get('schema') %}

-- Check that the Dynamic Table exists
SELECT 
  'Dynamic Table not found' AS error
QUALIFY NOT EXISTS (
  SELECT 1
  FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
  WHERE table_schema = '{{ fs_schema | upper }}'
    AND table_name = '"TEST_MANAGED_CUSTOMER_FEATURES$1.0"'
)
