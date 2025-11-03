# dbt Snowflake Feature Store

Professional dbt macros and materializations for building Snowflake Feature Store objects. This package lets you define entities and feature views as dbt models with full compatibility with `snowflake-ml-python` API.

## At a Glance

- **Materializations**: `entity`, `feature_view`
- **Warehouse**: Snowflake
- **dbt Compatibility**: dbt 1.6+
- **Feature Store Compatibility**: `snowflake-ml-python` 1.0+


## Quickstart (Developer Testing)

Follow these steps on macOS/Linux with Python 3 installed to test the package.

1. **Clone and enter the repo**

```bash
git clone https://github.com/your-org/dbt-snowflake-feature-store.git
cd dbt-snowflake-feature-store/
```

2. **Create an isolated Python environment and install dependencies**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install dbt-snowflake
```

3. **Configure Snowflake credentials**

Set the following environment variables. For username/password auth use `SNOWFLAKE_TEST_AUTHENTICATOR=snowflake`.

```bash
export SNOWFLAKE_TEST_ACCOUNT=<account>
export SNOWFLAKE_TEST_USER=<user>
export SNOWFLAKE_TEST_PASSWORD=<password>
export SNOWFLAKE_TEST_AUTHENTICATOR=<authenticator>   # e.g. snowflake | externalbrowser
export SNOWFLAKE_TEST_ROLE=<role>
export SNOWFLAKE_TEST_DATABASE=<database>
export SNOWFLAKE_TEST_WAREHOUSE=<warehouse>
export SNOWFLAKE_TEST_SCHEMA=<schema>
```

4. **Run integration tests**

```bash
cd integration_tests/
dbt deps
dbt build
```

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/your-org/dbt-snowflake-feature-store"
    revision: "v1.0.0"
```

Install the package:

```bash
dbt deps
```

## Configuration

Configure your Feature Store location in `dbt_project.yml`:

```yaml
vars:
  feature_store:
    schema: "FEATURE_STORE"
    default_warehouse: "COMPUTE_WH"
```

The database is automatically inferred from your `target.database`.

## Usage in Your dbt Project

### 1. Initialize Feature Store

Create the Feature Store schema and metadata tags:

```bash
dbt run-operation init_feature_store
```

### 2. Define an Entity

Create a model with `materialized='entity'`:

```sql
-- models/entities/customer.sql
{{
  config(
    materialized='entity',
    join_keys=['customer_id'],
    desc='Customer entity for profile and behavior features'
  )
}}

SELECT 1 WHERE FALSE
```

```bash
dbt run --select customer
```

### 3. Create a Feature View (Static)

Create a VIEW-based feature view for external data:

```sql
-- models/features/customer_profile.sql
{{
  config(
    materialized='feature_view',
    entities=['customer'],
    feature_view_version='1.0',
    timestamp_col='updated_at',
    desc='Customer demographic and profile features'
  )
}}

SELECT
  customer_id,
  updated_at,
  age AS f_age,
  country AS f_country,
  signup_date AS f_signup_date
FROM {{ ref('customers_base') }}
```

### 4. Create a Feature View (Managed)

Create a Dynamic Table feature view with automatic refresh:

```sql
-- models/features/customer_behavior.sql
{{
  config(
    materialized='feature_view',
    entities=['customer'],
    feature_view_version='1.0',
    timestamp_col='event_time',
    refresh_freq='5 minutes',
    warehouse='COMPUTE_WH',
    refresh_mode='AUTO',
    desc='Customer behavioral features'
  )
}}

SELECT
  customer_id,
  event_time,
  COUNT(*) AS f_num_orders,
  SUM(amount) AS f_total_spent
FROM {{ ref('orders') }}
GROUP BY customer_id, event_time
```

### 5. Deploy All

```bash
dbt run
```

## Usage with Python API

Feature views created by dbt work seamlessly with `snowflake-ml-python`:

```python
from snowflake.ml.feature_store import FeatureStore
from snowflake.snowpark import Session

session = Session.builder.configs(connection_params).create()
fs = FeatureStore(session, database='MY_DB', name='FEATURE_STORE', default_warehouse='COMPUTE_WH')

# Retrieve feature view
fv = fs.get_feature_view('customer_profile', '1.0')

# List all feature views
feature_views = fs.list_feature_views()

# Generate training dataset
training_df = fs.generate_dataset(
    name='customer_training',
    version='1.0',
    spine_df=spine_df,
    features=[fv],
    spine_timestamp_col='event_time'
)
```

## Configuration Reference

### Entity Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `materialized` | string | ✅ | Must be `'entity'` |
| `join_keys` | list[string] | ✅ | Column names used to join this entity |
| `desc` | string | ❌ | Human-readable description |

**Example:**

```sql
{{
  config(
    materialized='entity',
    join_keys=['customer_id', 'region'],
    desc='Customer segmented by region'
  )
}}
```

### Feature View Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `materialized` | string | ✅ | Must be `'feature_view'` |
| `entities` | list[string] | ✅ | Entity names (must be created first) |
| `feature_view_version` | string | ✅ | Semantic version (e.g., `'1.0'`, `'2.1'`) |
| `timestamp_col` | string | ❌ | Timestamp column for point-in-time joins |
| `refresh_freq` | string | ❌ | Refresh frequency (e.g., `'5 minutes'`, `'1 hour'`). Creates Dynamic Table if provided |
| `warehouse` | string | ⚠️ | Required if `refresh_freq` is set |
| `refresh_mode` | string | ❌ | `'AUTO'`, `'FULL'`, or `'INCREMENTAL'` (default: `'AUTO'`) |
| `initialize` | string | ❌ | `'ON_CREATE'` or `'ON_SCHEDULE'` (default: `'ON_CREATE'`) |
| `on_configuration_change` | string | ❌ | `'apply'`, `'continue'`, or `'fail'` (default: `'apply'`) |
| `desc` | string | ❌ | Human-readable description |

## Dynamic Table Change Management

When you specify `refresh_freq`, dbt creates a **Dynamic Table** instead of a VIEW. The package implements intelligent change detection:

### Configuration Changes (Non-Destructive)

Changes to `refresh_freq` or `warehouse` are applied via `ALTER DYNAMIC TABLE` (instant, no data recreation):

```bash
# Change refresh_freq in your model
# From: refresh_freq='5 minutes'
# To:   refresh_freq='10 minutes'

dbt run --select my_features
# → ALTER DYNAMIC TABLE SET TARGET_LAG = '10 minutes'
```

### Schema Changes (Destructive)

Changes to columns or query logic require `CREATE OR REPLACE` (full backfill):

```bash
# Add a new column to your SELECT
dbt run --select my_features
# → CREATE OR REPLACE DYNAMIC TABLE
```

Use `--full-refresh` to force recreation:

```bash
dbt run --select my_features --full-refresh
```

### Change Detection Modes

Control how configuration changes are handled:

```sql
{{
  config(
    on_configuration_change='apply'  -- Auto-apply config changes (default)
  )
}}
```

- `'apply'`: Automatically apply configuration changes via `ALTER`
- `'continue'`: Skip changes, log warning
- `'fail'`: Raise error, require `--full-refresh`

## Utility Macros

### List Entities

```bash
dbt run-operation list_entities
```

Output:
```
========================================
Registered Entities in Feature Store
========================================
  • CUSTOMER
    Join Keys: ["CUSTOMER_ID"]
    Description: Customer identifier

Total: 1 entities
========================================
```

## Project Structure

```
your-dbt-project/
├── dbt_project.yml
├── packages.yml
└── models/
    ├── entities/
    │   ├── customer.sql         # Entity definitions
    │   └── product.sql
    └── features/
        ├── customer_profile.sql # Feature view definitions
        └── product_stats.sql
```

## Testing

Run dbt tests:

```bash
dbt test
```

Run Python integration tests (validates compatibility with `snowflake-ml-python`):

```bash
cd integration_tests/python_tests
pip install -r requirements.txt
pytest -v
```

See `integration_tests/python_tests/README.md` for details.

## Requirements

- **Snowflake Account** with Feature Store enabled
- **dbt-snowflake** >= 1.6.0
- **snowflake-ml-python** >= 1.0.0 (for Python API usage)
- Appropriate Snowflake permissions for creating schemas, tags, views, and dynamic tables

## Best Practices

1. **Version Control:** Use semantic versioning for feature views (`'1.0'`, `'1.1'`, `'2.0'`)
2. **Entity First:** Always create entities before feature views that reference them
3. **Timestamp Columns:** Include timestamp columns for point-in-time correctness
4. **Feature Naming:** Prefix feature columns with `f_` (e.g., `f_age`, `f_total_spent`)
5. **Incremental Logic:** Use Dynamic Tables for features requiring periodic refresh
6. **State Comparison:** Use `dbt run --select state:modified` in production to only run changed models

## Limitations
- Online feature tables not yet supported

## Troubleshooting

### Entity not found error
**Symptom:** `Entity 'X' not found in Feature Store`

**Solution:** Ensure you've created the entity first:
```bash
dbt run --select <entity_name>
```

### Dynamic Table won't refresh
**Symptom:** Dynamic Table shows `SUSPENDED` or not updating

**Solution:** Check warehouse permissions and state:
```sql
SHOW DYNAMIC TABLES IN SCHEMA <schema_name>;
```

### UI Error: "Cannot read properties of undefined (reading 'joinKeys')"
**Symptom:** Snowsight UI shows JavaScript error when viewing Feature Store

**Root Cause:** Old feature views created before metadata fix have entities as strings instead of objects.

**Solution:**

1. **Find bad feature views:**
   ```bash
   python scripts/cleanup_old_metadata.py --check
   ```

2. **Drop and recreate them:**
   ```bash
   python scripts/cleanup_old_metadata.py --drop
   dbt run --full-refresh  # Recreate with correct metadata
   ```

3. **Or manually drop:**
   ```sql
   DROP VIEW/DYNAMIC TABLE <old_feature_view>;
   ```

4. **Refresh the UI** (Ctrl+R or Cmd+R)

**Prevention:** Always use the latest version of this package to ensure correct metadata structure.

### Tags not visible in Python API
**Symptom:** `get_feature_view()` fails or returns None

**Solution:** Verify tags exist:
```sql
SHOW TAGS IN SCHEMA <schema_name>;
SELECT * FROM INFORMATION_SCHEMA.TAG_REFERENCES 
WHERE OBJECT_SCHEMA = '<schema_name>';
```

## License

Apache 2.0

## Support

- **Issues:** [GitHub Issues](https://github.com/your-org/dbt-snowflake-feature-store/issues)
- **Documentation:** See inline code comments and integration tests
- **Contributing:** Pull requests welcome!
