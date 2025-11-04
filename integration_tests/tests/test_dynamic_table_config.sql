-- Test that Dynamic Table has correct configuration

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = target.database %}
{% set fs_schema = fs_config.get('schema') %}

-- Validate Dynamic Table configuration
SELECT 
  table_name,
  target_lag,
  refresh_mode,
  'Expected: TARGET_LAG = 1 minute, REFRESH_MODE = AUTO' AS error_message
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
WHERE table_schema = '{{ fs_schema | upper }}'
  AND table_name = '"TEST_MANAGED_CUSTOMER_FEATURES$1.0"'
  AND (
    target_lag != '1 minute'
    OR refresh_mode != 'AUTO'
  )


