# Python Integration Tests

Integration tests that validate dbt-created Feature Store objects work correctly with the Snowflake ML Python API.

## 🎯 Why These Tests Are Critical

These tests ensure **byte-for-byte metadata compatibility** between dbt-created objects and Python API-created objects. They have caught critical bugs:

- **Bug Found:** Entities stored as objects `[{"name": "X", "joinKeys": [...]}]` instead of strings `["X"]`
- **Impact:** Snowsight UI crashed with `TypeError: Cannot read properties of undefined (reading 'joinKeys')`
- **How Caught:** `test_metadata_compatibility.py` compares metadata structure with Python API reference

**Key Learning:** Testing that the Python API can "read" feature views is NOT enough. We must validate the **exact metadata structure** matches what the Python API generates.

## Setup

1. **Install dependencies:**

```bash
cd integration_tests/python_tests
pip install -r requirements.txt
```

2. **Set environment variables:**

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="your_role"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="FEATURE_STORE"
```

Or create a `.env` file (not committed):

```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=FEATURE_STORE
```

## Running Tests

### Prerequisites

**First, run dbt to create the Feature Store objects:**

```bash
cd ../..  # Back to package root

# Initialize Feature Store
dbt run-operation init_feature_store

# Create entities
dbt run --select tag:entity

# Create feature views
dbt run --select tag:feature_view
```

### Run Python Tests

```bash
cd integration_tests/python_tests
pytest -v
```

### Run Specific Test Classes

```bash
# Test entity discovery
pytest test_feature_store_api.py::TestEntityDiscovery -v

# Test feature view discovery
pytest test_feature_store_api.py::TestFeatureViewDiscovery -v

# Test feature retrieval
pytest test_feature_store_api.py::TestFeatureRetrieval -v

# Test Dynamic Table behavior
pytest test_feature_store_api.py::TestDynamicTableBehavior -v
```

## What These Tests Validate

### Entity Tests
- ✅ Entities created by dbt are discoverable via `feature_store.list_entities()`
- ✅ Entities can be retrieved via `feature_store.get_entity()`
- ✅ Entity metadata (join_keys, description) is correct

### Feature View Tests
- ✅ Feature views created by dbt are discoverable via `feature_store.list_feature_views()`
- ✅ Static (VIEW) feature views can be retrieved and read
- ✅ Managed (DYNAMIC TABLE) feature views can be retrieved and read
- ✅ Feature view metadata (entities, timestamp_col) is correct
- ✅ Dynamic Tables have correct refresh configuration

### Metadata Tests
- ✅ TAG format is compatible with Python API
- ✅ JSON structure matches expected schema

### Dynamic Table Tests
- ✅ Dynamic Table was created (not a VIEW)
- ✅ TARGET_LAG, WAREHOUSE, REFRESH_MODE are correct
- ✅ Subsequent dbt runs don't recreate the table

## Test Output

Expected output:

```
test_feature_store_api.py::TestEntityDiscovery::test_list_entities PASSED
test_feature_store_api.py::TestEntityDiscovery::test_get_entity PASSED
test_feature_store_api.py::TestFeatureViewDiscovery::test_list_feature_views PASSED
test_feature_store_api.py::TestFeatureViewDiscovery::test_get_static_feature_view PASSED
test_feature_store_api.py::TestFeatureViewDiscovery::test_get_managed_feature_view PASSED
test_feature_store_api.py::TestFeatureRetrieval::test_read_static_feature_view PASSED
test_feature_store_api.py::TestFeatureRetrieval::test_read_managed_feature_view PASSED
test_feature_store_api.py::TestMetadataCompatibility::test_entity_metadata_format PASSED
test_feature_store_api.py::TestMetadataCompatibility::test_feature_view_metadata_format PASSED
test_feature_store_api.py::TestDynamicTableBehavior::test_dynamic_table_exists PASSED
test_feature_store_api.py::TestDynamicTableBehavior::test_dynamic_table_not_recreated_on_normal_run PASSED

======================== 11 passed in 15.42s ========================
```

## Troubleshooting

### Connection Issues

```
snowflake.connector.errors.DatabaseError: 250001: Could not connect to Snowflake backend
```

**Fix:** Check your environment variables and credentials.

### Feature Views Not Found

```
AssertionError: assert 'TEST_STATIC_CUSTOMER_FEATURES' in fv_names
```

**Fix:** Run dbt first to create the feature views:
```bash
dbt run --select tag:feature_view
```

### Schema Not Found

```
Object 'FEATURE_STORE' does not exist
```

**Fix:** Initialize the Feature Store:
```bash
dbt run-operation init_feature_store
```


