-- Test that feature view metadata contains properly structured entity objects with joinKeys
-- This validates the fix for UI error: "Cannot read properties of undefined (reading 'joinKeys')"

WITH metadata_tags AS (
    SELECT 
        object_name,
        tag_name,
        tag_value
    FROM snowflake.account_usage.tag_references
    WHERE tag_database = '{{ target.database }}'
      AND tag_schema = '{{ var("feature_store")["schema"] }}'
      AND tag_name = 'SNOWML_FEATURE_VIEW_METADATA'
      AND object_name IN ('TEST_STATIC_CUSTOMER_FEATURES$1_0', 'TEST_MANAGED_CUSTOMER_FEATURES$1_0')
),

parsed_metadata AS (
    SELECT 
        object_name,
        tag_value,
        PARSE_JSON(tag_value) AS metadata_json,
        metadata_json:entities AS entities_array
    FROM metadata_tags
),

entity_validation AS (
    SELECT 
        object_name,
        entity.value:name::STRING AS entity_name,
        entity.value:joinKeys AS join_keys_array,
        ARRAY_SIZE(entity.value:joinKeys) AS join_keys_count
    FROM parsed_metadata,
    LATERAL FLATTEN(input => entities_array) entity
)

-- Validate that all entities have the required structure
SELECT 
    object_name,
    entity_name,
    join_keys_array,
    join_keys_count
FROM entity_validation
WHERE 
    -- Entity name must exist
    entity_name IS NULL
    -- joinKeys must be an array
    OR join_keys_array IS NULL
    -- joinKeys must have at least one key
    OR join_keys_count = 0
    -- Entity name should be uppercase (Snowflake convention)
    OR entity_name != UPPER(entity_name)

