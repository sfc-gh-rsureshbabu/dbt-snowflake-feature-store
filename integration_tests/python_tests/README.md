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

## Test Suites

### 1. `test_metadata_compatibility.py` ⭐ **Most Critical**

**Metadata structure validation** - Ensures byte-for-byte compatibility with Python API.

Tests:
- `test_metadata_structure_matches_python_api()` - Compares with Python API-created metadata
- `test_entities_are_uppercase_strings()` - Validates entity format (strings, not objects)
- `test_timestamp_col_format()` - Validates timestamp column metadata
- `test_ui_can_parse_metadata()` - Simulates Snowsight UI parsing

**Why Critical:** Caught the bug where entities were objects `[{name, joinKeys}]` instead of strings `["NAME"]`, which broke the UI.

### 2. `test_feature_store_workflows.py` 🔄 **End-to-End**

**Full API workflow validation** - Tests real-world Feature Store usage.

**Dataset Generation:**
- `test_generate_dataset_single_feature_view()` - Basic dataset creation from dbt FV
- `test_point_in_time_correctness()` - Validates temporal joins work correctly
- `test_generate_dataset_multiple_feature_views()` - Multi-FV joins

**Training Datasets:**
- `test_create_and_retrieve_dataset()` - Full lifecycle: create → save → retrieve → read
- `test_dataset_metadata()` - Dataset metadata validation

**Feature View Chaining:**
- `test_feature_view_references_another_fv()` - Base FV → Derived FV pattern
- `test_multi_hop_feature_view_chain()` - Multi-level chains (base → intermediate → final)

**Dynamic Tables:**
- `test_generate_dataset_from_dynamic_table_fv()` - Managed FV in datasets
- `test_mix_static_and_managed_fvs()` - Mixed VIEW + Dynamic Table datasets

### 3. `test_feature_store_api.py` 📋 **Basic Compatibility**

**API discovery and retrieval** - Tests basic Feature Store operations.

**Entity Tests:**
- ✅ Entities discoverable via `list_entities()`
- ✅ Entity retrieval via `get_entity()`
- ✅ Entity metadata (join_keys, description) correct

**Feature View Tests:**
- ✅ Feature views discoverable via `list_feature_views()`
- ✅ Static (VIEW) FVs can be retrieved and read
- ✅ Managed (DYNAMIC TABLE) FVs can be retrieved and read
- ✅ FV metadata (entities, timestamp_col) correct

**Dynamic Table Tests:**
- ✅ Dynamic Table created (not a VIEW)
- ✅ TARGET_LAG, WAREHOUSE, REFRESH_MODE correct
- ✅ Subsequent dbt runs don't recreate

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


