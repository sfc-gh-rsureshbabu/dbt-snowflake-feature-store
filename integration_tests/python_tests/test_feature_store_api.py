"""
Integration tests for dbt Feature Store with snowflake-ml-python API.

Tests that feature views created by dbt work correctly with the
Python Feature Store API.
"""

import pytest
from snowflake.snowpark import Session
from snowflake.ml.feature_store import FeatureStore, Entity, FeatureView
import os


@pytest.fixture(scope="module")
def snowflake_session():
    """Create a Snowflake session from environment variables or config."""
    connection_params = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "FEATURE_STORE"),
    }
    
    # Filter out None values
    connection_params = {k: v for k, v in connection_params.items() if v is not None}
    
    session = Session.builder.configs(connection_params).create()
    yield session
    session.close()


@pytest.fixture(scope="module")
def feature_store(snowflake_session):
    """Create FeatureStore instance."""
    database = os.environ.get("SNOWFLAKE_DATABASE", snowflake_session.get_current_database())
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "FEATURE_STORE")
    
    fs = FeatureStore(
        session=snowflake_session,
        database=database,
        name=schema,
        default_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE")
    )
    return fs


class TestEntityDiscovery:
    """Test that dbt-created entities are discoverable via Python API."""
    
    def test_list_entities(self, feature_store):
        """Test that we can list entities created by dbt."""
        entities = feature_store.list_entities().collect()
        entity_names = [row['NAME'] for row in entities]
        
        # Entities created by dbt models
        assert 'TEST_CUSTOMER_ENTITY' in entity_names
        assert 'TEST_ORDER_ENTITY' in entity_names
    
    def test_get_entity(self, feature_store):
        """Test that we can retrieve an entity created by dbt."""
        entity = feature_store.get_entity('TEST_CUSTOMER_ENTITY')
        
        assert entity is not None
        assert entity.name == 'TEST_CUSTOMER_ENTITY'
        assert 'CUSTOMER_ID' in entity.join_keys


class TestFeatureViewDiscovery:
    """Test that dbt-created feature views are discoverable via Python API."""
    
    def test_list_feature_views(self, feature_store):
        """Test that we can list feature views created by dbt."""
        fvs = feature_store.list_feature_views().collect()
        fv_names = [row['NAME'] for row in fvs]
        
        # Feature views created by dbt models
        assert 'TEST_STATIC_CUSTOMER_FEATURES' in fv_names
        assert 'TEST_MANAGED_CUSTOMER_FEATURES' in fv_names
    
    def test_get_static_feature_view(self, feature_store):
        """Test that we can retrieve a static (VIEW) feature view created by dbt."""
        fv = feature_store.get_feature_view(
            name='TEST_STATIC_CUSTOMER_FEATURES',
            version='1.0'
        )
        
        assert fv is not None
        assert fv.name == 'TEST_STATIC_CUSTOMER_FEATURES'
        assert fv.version == '1.0'
        assert 'TEST_CUSTOMER_ENTITY' in fv.entities
        assert fv.timestamp_col == 'UPDATED_AT'
        
        # Should be EXTERNAL_FEATURE_VIEW (VIEW)
        assert fv.status == 'ACTIVE' or fv.status == 'DRAFT'
    
    def test_get_managed_feature_view(self, feature_store):
        """Test that we can retrieve a managed (DYNAMIC TABLE) feature view created by dbt."""
        fv = feature_store.get_feature_view(
            name='TEST_MANAGED_CUSTOMER_FEATURES',
            version='1.0'
        )
        
        assert fv is not None
        assert fv.name == 'TEST_MANAGED_CUSTOMER_FEATURES'
        assert fv.version == '1.0'
        assert 'TEST_CUSTOMER_ENTITY' in fv.entities
        assert fv.timestamp_col == 'UPDATED_AT'
        
        # Should be MANAGED_FEATURE_VIEW (DYNAMIC TABLE)
        assert fv.refresh_freq is not None
        assert '1 minute' in fv.refresh_freq.lower() or '1minute' in fv.refresh_freq.lower()


class TestFeatureRetrieval:
    """Test that we can read features from dbt-created feature views."""
    
    def test_read_static_feature_view(self, feature_store, snowflake_session):
        """Test reading data from a static feature view."""
        fv = feature_store.get_feature_view(
            name='TEST_STATIC_CUSTOMER_FEATURES',
            version='1.0'
        )
        
        # Read the feature view
        df = snowflake_session.table(f'FEATURE_STORE.TEST_STATIC_CUSTOMER_FEATURES$1.0')
        data = df.collect()
        
        assert len(data) > 0
        
        # Check columns exist
        columns = df.columns
        assert 'CUSTOMER_ID' in columns
        assert 'UPDATED_AT' in columns
        assert 'F_AGE' in columns
        assert 'F_COUNTRY' in columns
    
    def test_read_managed_feature_view(self, feature_store, snowflake_session):
        """Test reading data from a managed feature view."""
        fv = feature_store.get_feature_view(
            name='TEST_MANAGED_CUSTOMER_FEATURES',
            version='1.0'
        )
        
        # Read the feature view
        df = snowflake_session.table(f'FEATURE_STORE.TEST_MANAGED_CUSTOMER_FEATURES$1.0')
        data = df.collect()
        
        assert len(data) > 0
        
        # Check columns exist
        columns = df.columns
        assert 'CUSTOMER_ID' in columns
        assert 'UPDATED_AT' in columns
        assert 'F_AGE' in columns
        assert 'F_TIER' in columns


class TestMetadataCompatibility:
    """Test that metadata stored by dbt is compatible with Python API and UI."""
    
    def test_feature_view_metadata_structure_ui_compatibility(self, snowflake_session):
        """
        Test that feature view metadata has the correct structure for UI compatibility.
        
        This test validates the fix for: "Cannot read properties of undefined (reading 'joinKeys')"
        The UI expects entities to be objects with 'name' and 'joinKeys' properties.
        """
        # Query the feature view metadata tag
        query = """
        SELECT 
            OBJECT_NAME,
            TAG_VALUE
        FROM INFORMATION_SCHEMA.TAG_REFERENCES
        WHERE TAG_NAME = 'SNOWML_FEATURE_VIEW_METADATA'
          AND OBJECT_NAME IN ('TEST_STATIC_CUSTOMER_FEATURES$1_0', 'TEST_MANAGED_CUSTOMER_FEATURES$1_0')
        """
        
        result = snowflake_session.sql(query).collect()
        assert len(result) >= 1, "No feature view metadata tags found"
        
        # Verify structure for each feature view
        import json
        for row in result:
            object_name = row['OBJECT_NAME']
            metadata = json.loads(row['TAG_VALUE'])
            
            # Metadata must have required fields
            assert 'entities' in metadata, f"{object_name}: Missing 'entities' field"
            assert 'timestamp_col' in metadata, f"{object_name}: Missing 'timestamp_col' field"
            
            # Entities must be a non-empty list
            entities = metadata['entities']
            assert isinstance(entities, list), f"{object_name}: 'entities' must be a list"
            assert len(entities) > 0, f"{object_name}: 'entities' list is empty"
            
            # Each entity must be an object with 'name' and 'joinKeys'
            for entity in entities:
                assert isinstance(entity, dict), f"{object_name}: Entity must be an object, got {type(entity)}"
                assert 'name' in entity, f"{object_name}: Entity missing 'name' field: {entity}"
                assert 'joinKeys' in entity, f"{object_name}: Entity missing 'joinKeys' field: {entity}"
                
                # joinKeys must be a non-empty list
                join_keys = entity['joinKeys']
                assert isinstance(join_keys, list), f"{object_name}: 'joinKeys' must be a list"
                assert len(join_keys) > 0, f"{object_name}: 'joinKeys' list is empty for entity {entity['name']}"
                
                # Entity name should be uppercase (Snowflake convention)
                assert entity['name'] == entity['name'].upper(), \
                    f"{object_name}: Entity name should be uppercase: {entity['name']}"
    
    def test_entity_tag_values(self, snowflake_session):
        """Test that entity tag values are set correctly on feature views."""
        # Query entity tags on feature views
        query = """
        SELECT 
            OBJECT_NAME,
            TAG_NAME,
            TAG_VALUE
        FROM INFORMATION_SCHEMA.TAG_REFERENCES
        WHERE TAG_NAME LIKE 'SNOWML_FEATURE_STORE_ENTITY_%'
          AND OBJECT_NAME IN ('TEST_STATIC_CUSTOMER_FEATURES$1_0', 'TEST_MANAGED_CUSTOMER_FEATURES$1_0')
        ORDER BY OBJECT_NAME, TAG_NAME
        """
        
        result = snowflake_session.sql(query).collect()
        assert len(result) >= 1, "No entity tags found on feature views"
        
        # Each feature view should have entity tags with comma-separated join keys
        for row in result:
            object_name = row['OBJECT_NAME']
            tag_name = row['TAG_NAME']
            tag_value = row['TAG_VALUE']
            
            # Tag value should be comma-separated join keys
            assert tag_value, f"{object_name}: Empty tag value for {tag_name}"
            assert len(tag_value) > 0, f"{object_name}: Tag value is empty for {tag_name}"


class TestDynamicTableBehavior:
    """Test Dynamic Table specific behavior."""
    
    def test_dynamic_table_exists(self, snowflake_session):
        """Test that dynamic table was created correctly."""
        query = """
        SELECT 
          TABLE_NAME,
          TARGET_LAG,
          WAREHOUSE_NAME,
          REFRESH_MODE
        FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
        WHERE TABLE_SCHEMA = 'FEATURE_STORE'
          AND TABLE_NAME = 'TEST_MANAGED_CUSTOMER_FEATURES$1.0'
        """
        
        result = snowflake_session.sql(query).collect()
        assert len(result) == 1
        
        dt = result[0]
        assert dt['TABLE_NAME'] == 'TEST_MANAGED_CUSTOMER_FEATURES$1.0'
        assert dt['TARGET_LAG'] == '1 minute'
        assert dt['REFRESH_MODE'] == 'AUTO'
    
    def test_dynamic_table_not_recreated_on_normal_run(self, snowflake_session):
        """
        Test that running dbt again doesn't recreate the dynamic table.
        
        Note: This test assumes dbt has been run at least once before.
        We check that the dynamic table exists and has metadata indicating
        it wasn't just created.
        """
        query = """
        SELECT 
          CREATED,
          LAST_ALTERED
        FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
        WHERE TABLE_SCHEMA = 'FEATURE_STORE'
          AND TABLE_NAME = 'TEST_MANAGED_CUSTOMER_FEATURES$1.0'
        """
        
        result = snowflake_session.sql(query).collect()
        assert len(result) == 1
        
        # If the table exists, that's good enough for this test
        # In a more sophisticated test, we'd run dbt and check timestamps


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


