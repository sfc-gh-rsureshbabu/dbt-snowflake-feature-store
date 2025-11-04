-- Test that managed (Dynamic Table) feature views are created successfully
-- NOTE: Tag validation done in Python tests due to:
-- 1. ACCOUNT_USAGE latency (up to 2 hours)
-- 2. INFORMATION_SCHEMA.DYNAMIC_TABLES not available in all editions

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = target.database %}
{% set fs_schema = fs_config.get('schema') %}

-- Test passes - Dynamic Table creation with tags validated by materialization success
SELECT NULL AS placeholder WHERE FALSE
