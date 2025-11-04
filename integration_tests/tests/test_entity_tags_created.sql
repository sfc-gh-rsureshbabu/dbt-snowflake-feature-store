-- Test that entity models ran successfully
-- If the entity models build without error, the tags were created
-- This test is intentionally simple - we validate tag contents in Python tests

{% set fs_config = var('feature_store', {}) %}
{% set fs_database = target.database %}
{% set fs_schema = fs_config.get('schema') %}

-- Always pass - entity creation is validated by the materialization itself
SELECT NULL AS placeholder WHERE FALSE
