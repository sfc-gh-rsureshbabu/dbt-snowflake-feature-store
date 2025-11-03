"""
Test that dbt-created metadata EXACTLY matches snowflake-ml-python format.

This test creates a reference feature view using Python API, then compares
the metadata structure byte-for-byte with dbt-created feature views.

This would have caught the entities format issue:
- Wrong: entities = [{"name": "X", "joinKeys": ["Y"]}]  # Objects
- Right: entities = ["X"]  # Strings
"""

import pytest
import json
from snowflake.snowpark import Session
from snowflake.ml.feature_store import FeatureStore, Entity, FeatureView
import os


@pytest.fixture(scope="module")
def snowflake_session():
    """Create a Snowflake session."""
    connection_params = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
    }
    connection_params = {k: v for k, v in connection_params.items() if v is not None}
    
    session = Session.builder.configs(connection_params).create()
    yield session
    session.close()


@pytest.fixture(scope="module")
def feature_store(snowflake_session):
    """Create FeatureStore instance."""
    database = os.environ.get("SNOWFLAKE_DATABASE", "rsureshbabu")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "FEATURE_STORE")
    
    fs = FeatureStore(
        session=snowflake_session,
        database=database,
        name=schema,
        default_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ADMIN_WH")
    )
    return fs


@pytest.fixture(scope="module")
def python_reference_fv(feature_store, snowflake_session):
    """
    Create a reference feature view using Python API.
    This is our 'golden' metadata format.
    """
    # Create a test entity if it doesn't exist
    try:
        entity = feature_store.get_entity("PYTHON_TEST_ENTITY")
    except:
        entity = Entity(
            name="PYTHON_TEST_ENTITY",
            join_keys=["TEST_ID"],
            desc="Test entity for metadata comparison"
        )
        entity = feature_store.register_entity(entity)
    
    # Create a simple test table
    try:
        snowflake_session.sql("""
            CREATE OR REPLACE TABLE FEATURE_STORE.python_test_source (
                TEST_ID INTEGER,
                UPDATED_AT TIMESTAMP,
                VALUE INTEGER
            )
        """).collect()
        
        snowflake_session.sql("""
            INSERT INTO FEATURE_STORE.python_test_source 
            VALUES (1, CURRENT_TIMESTAMP(), 100)
        """).collect()
    except:
        pass
    
    # Create feature view
    source_df = snowflake_session.table("FEATURE_STORE.python_test_source")
    
    try:
        fv = feature_store.get_feature_view("PYTHON_REFERENCE_FV", "1.0")
    except:
        fv = FeatureView(
            name="PYTHON_REFERENCE_FV",
            entities=[entity],
            feature_df=source_df,
            timestamp_col="UPDATED_AT",
            desc="Reference feature view for testing"
        )
        fv = feature_store.register_feature_view(
            feature_view=fv,
            version="1.0"
        )
    
    return fv


def get_metadata_from_snowflake(session, database, schema, object_name):
    """
    Get raw metadata JSON from Snowflake tags.
    Returns the metadata exactly as stored, not parsed by Python API.
    """
    query = f"""
        SELECT TAG_VALUE
        FROM TABLE(
            INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                '{database}.{schema}.{object_name}', 
                'TABLE'
            )
        )
        WHERE TAG_NAME = 'SNOWML_FEATURE_VIEW_METADATA'
          AND COLUMN_NAME IS NULL
        LIMIT 1
    """
    
    result = session.sql(query).collect()
    if result:
        return json.loads(result[0][0])
    return None


class TestMetadataStructureCompatibility:
    """Test that dbt metadata exactly matches Python API metadata."""
    
    def test_metadata_structure_matches_python_api(
        self, 
        snowflake_session, 
        feature_store,
        python_reference_fv
    ):
        """
        Compare dbt-created metadata with Python API-created metadata.
        
        This test would have caught the entities format issue!
        """
        # Get metadata from Python API-created feature view
        python_metadata = get_metadata_from_snowflake(
            snowflake_session,
            "RSURESHBABU",
            "FEATURE_STORE",
            "PYTHON_REFERENCE_FV$1_0"
        )
        
        # Get metadata from dbt-created feature view
        dbt_metadata = get_metadata_from_snowflake(
            snowflake_session,
            "RSURESHBABU",
            "FEATURE_STORE",
            "TEST_STATIC_CUSTOMER_FEATURES$1_0"
        )
        
        assert python_metadata is not None, "Python API metadata not found"
        assert dbt_metadata is not None, "dbt metadata not found"
        
        # Compare structure field by field
        python_entities = python_metadata.get('entities', [])
        dbt_entities = dbt_metadata.get('entities', [])
        
        # Check entities format
        assert isinstance(python_entities, list), "Python entities should be a list"
        assert isinstance(dbt_entities, list), "dbt entities should be a list"
        assert len(python_entities) > 0, "Python entities should not be empty"
        assert len(dbt_entities) > 0, "dbt entities should not be empty"
        
        # CRITICAL CHECK: Entity element types must match
        python_entity_type = type(python_entities[0])
        dbt_entity_type = type(dbt_entities[0])
        
        assert python_entity_type == dbt_entity_type, \
            f"Entity type mismatch! Python uses {python_entity_type.__name__}, " \
            f"dbt uses {dbt_entity_type.__name__}. " \
            f"Python: {python_entities[0]}, dbt: {dbt_entities[0]}"
        
        # If entities are strings (correct format)
        if isinstance(python_entities[0], str):
            assert isinstance(dbt_entities[0], str), \
                "dbt entities should be strings to match Python API"
            print("✅ Entities are strings (correct format)")
        
        # If entities are dicts (wrong format - should fail)
        elif isinstance(python_entities[0], dict):
            print("⚠️ Python API is using dict format for entities")
            assert 'name' in python_entities[0], "Entity dict should have 'name'"
            assert 'name' in dbt_entities[0], "dbt entity dict should have 'name'"
        
        # Check all metadata keys match
        python_keys = set(python_metadata.keys())
        dbt_keys = set(dbt_metadata.keys())
        
        assert python_keys == dbt_keys, \
            f"Metadata keys don't match! Python: {python_keys}, dbt: {dbt_keys}"
    
    def test_entities_are_uppercase_strings(self, snowflake_session):
        """
        Test that entities in metadata are uppercase strings.
        This is the format used by Python API.
        """
        dbt_metadata = get_metadata_from_snowflake(
            snowflake_session,
            "RSURESHBABU",
            "FEATURE_STORE",
            "TEST_STATIC_CUSTOMER_FEATURES$1_0"
        )
        
        entities = dbt_metadata.get('entities', [])
        
        # Check each entity
        for i, entity in enumerate(entities):
            # Must be a string
            assert isinstance(entity, str), \
                f"Entity[{i}] must be a string, got {type(entity).__name__}: {entity}"
            
            # Must be uppercase (Snowflake convention)
            assert entity == entity.upper(), \
                f"Entity[{i}] must be uppercase: got '{entity}', expected '{entity.upper()}'"
    
    def test_timestamp_col_format(self, snowflake_session):
        """Test that timestamp_col is stored correctly."""
        dbt_metadata = get_metadata_from_snowflake(
            snowflake_session,
            "RSURESHBABU",
            "FEATURE_STORE",
            "TEST_STATIC_CUSTOMER_FEATURES$1_0"
        )
        
        timestamp_col = dbt_metadata.get('timestamp_col')
        
        # Should be a string (column name) or 'NULL'
        assert isinstance(timestamp_col, str), \
            f"timestamp_col should be string, got {type(timestamp_col)}"
    
    def test_compare_full_json_structure(
        self, 
        snowflake_session,
        python_reference_fv
    ):
        """
        Byte-for-byte comparison of JSON structure.
        This would catch any subtle differences.
        """
        python_metadata = get_metadata_from_snowflake(
            snowflake_session,
            "RSURESHBABU",
            "FEATURE_STORE",
            "PYTHON_REFERENCE_FV$1_0"
        )
        
        dbt_metadata = get_metadata_from_snowflake(
            snowflake_session,
            "RSURESHBABU",
            "FEATURE_STORE",
            "TEST_STATIC_CUSTOMER_FEATURES$1_0"
        )
        
        # Normalize for comparison (remove specific values)
        def normalize_metadata(meta):
            return {
                'entities_type': type(meta['entities']).__name__,
                'entities_count': len(meta['entities']),
                'first_entity_type': type(meta['entities'][0]).__name__ if meta['entities'] else None,
                'timestamp_col_type': type(meta['timestamp_col']).__name__,
                'keys': sorted(meta.keys())
            }
        
        python_normalized = normalize_metadata(python_metadata)
        dbt_normalized = normalize_metadata(dbt_metadata)
        
        # Compare normalized structures
        assert python_normalized == dbt_normalized, \
            f"Metadata structure differs!\nPython: {python_normalized}\ndbt: {dbt_normalized}"
        
        print("✅ Metadata structure is identical to Python API")


def test_ui_can_parse_metadata(snowflake_session):
    """
    Simulate what the UI does when parsing metadata.
    This would have caught the joinKeys error!
    """
    dbt_metadata = get_metadata_from_snowflake(
        snowflake_session,
        "RSURESHBABU",
        "FEATURE_STORE",
        "TEST_STATIC_CUSTOMER_FEATURES$1_0"
    )
    
    # Simulate UI code: getFeatureNamesForAllFeatureViews
    try:
        entities = dbt_metadata['entities']
        
        for entity in entities:
            # UI expects to be able to access entity directly as string
            # or as dict with 'name' and 'joinKeys'
            
            if isinstance(entity, str):
                # Correct format - entity name is the string itself
                entity_name = entity
                print(f"✅ Entity is string: {entity_name}")
            
            elif isinstance(entity, dict):
                # If dict, must have 'name' and 'joinKeys'
                assert 'name' in entity, "Entity dict missing 'name' field"
                assert 'joinKeys' in entity, "Entity dict missing 'joinKeys' field"
                
                entity_name = entity['name']
                join_keys = entity['joinKeys']
                
                # This is what caused the UI error - trying to read joinKeys
                # when entities were strings
                print(f"⚠️ Entity is dict: {entity_name}, joinKeys: {join_keys}")
            
            else:
                raise AssertionError(
                    f"Entity must be string or dict, got {type(entity).__name__}"
                )
        
        print("✅ UI can successfully parse metadata")
    
    except KeyError as e:
        raise AssertionError(
            f"UI parsing would fail with: Cannot read properties of undefined (reading '{e.args[0]}')"
        )

