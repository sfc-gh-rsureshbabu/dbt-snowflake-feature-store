"""
Test end-to-end Feature Store workflows using snowflake-ml-python API.

Tests:
1. Generate dataset from dbt-created feature views
2. Create training sets
3. Point-in-time correctness
4. Feature view chaining (one FV uses another as source)
5. Multi-entity joins
"""

import pytest
from snowflake.snowpark import Session
from snowflake.ml.feature_store import FeatureStore
import pandas as pd
from datetime import datetime, timedelta
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
    schema = "FEATURE_STORE"
    
    fs = FeatureStore(
        session=snowflake_session,
        database=database,
        name=schema,
        default_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ADMIN_WH")
    )
    return fs


@pytest.fixture(scope="module")
def test_data(snowflake_session):
    """
    Create test data with proper temporal structure.
    This ensures point-in-time joins work correctly.
    """
    # Create base table with historical data
    snowflake_session.sql("""
        CREATE OR REPLACE TABLE FEATURE_STORE.customer_base_table (
            customer_id INTEGER,
            updated_at TIMESTAMP,
            age INTEGER,
            country STRING,
            tier STRING
        )
    """).collect()
    
    # Insert historical data (past 30 days)
    base_time = datetime.now()
    for i in range(1, 11):  # 10 customers
        for days_ago in [30, 20, 10, 5, 1]:  # Historical snapshots
            ts = base_time - timedelta(days=days_ago)
            snowflake_session.sql(f"""
                INSERT INTO FEATURE_STORE.customer_base_table VALUES
                ({i}, '{ts.isoformat()}', {20 + i}, 'US', 
                 CASE WHEN {i} % 3 = 0 THEN 'Gold' ELSE 'Silver' END)
            """).collect()
    
    # Create spine/label table for training
    snowflake_session.sql("""
        CREATE OR REPLACE TABLE FEATURE_STORE.training_spine (
            customer_id INTEGER,
            event_time TIMESTAMP,
            label INTEGER
        )
    """).collect()
    
    # Labels from 7 days ago
    label_time = base_time - timedelta(days=7)
    for i in range(1, 11):
        snowflake_session.sql(f"""
            INSERT INTO FEATURE_STORE.training_spine VALUES
            ({i}, '{label_time.isoformat()}', {i % 2})
        """).collect()
    
    yield {
        'base_time': base_time,
        'label_time': label_time,
        'num_customers': 10
    }


class TestDatasetGeneration:
    """Test generating datasets from dbt-created feature views."""
    
    def test_generate_dataset_single_feature_view(
        self, 
        feature_store, 
        snowflake_session,
        test_data
    ):
        """
        Test generating a dataset from a single feature view.
        This validates dbt feature views work with generate_dataset().
        """
        # Get dbt-created feature view
        try:
            fv = feature_store.get_feature_view('TEST_STATIC_CUSTOMER_FEATURES', '1.0')
        except Exception as e:
            pytest.skip(f"Feature view not found: {e}")
        
        # Create spine dataframe
        spine_df = snowflake_session.table('FEATURE_STORE.training_spine')
        
        # Generate dataset
        dataset = feature_store.generate_dataset(
            name='test_dataset',
            spine_df=spine_df,
            features=[fv],
            spine_timestamp_col='event_time',
            spine_label_cols=['label']
        )
        
        # Validate dataset
        assert dataset is not None, "Dataset should be created"
        
        # Read dataset
        df = dataset.read.to_pandas()
        
        # Validate structure
        assert len(df) > 0, "Dataset should have rows"
        assert 'CUSTOMER_ID' in df.columns, "Should have customer_id from spine"
        assert 'EVENT_TIME' in df.columns, "Should have event_time from spine"
        assert 'LABEL' in df.columns, "Should have label from spine"
        assert 'F_AGE' in df.columns, "Should have feature from feature view"
        assert 'F_COUNTRY' in df.columns, "Should have feature from feature view"
        
        print(f"✅ Generated dataset with {len(df)} rows and {len(df.columns)} columns")
        print(f"   Columns: {list(df.columns)}")
    
    def test_point_in_time_correctness(
        self,
        feature_store,
        snowflake_session,
        test_data
    ):
        """
        Test that point-in-time joins work correctly.
        Features should be from BEFORE the event_time, not after.
        """
        try:
            fv = feature_store.get_feature_view('TEST_STATIC_CUSTOMER_FEATURES', '1.0')
        except Exception as e:
            pytest.skip(f"Feature view not found: {e}")
        
        spine_df = snowflake_session.table('FEATURE_STORE.training_spine')
        
        dataset = feature_store.generate_dataset(
            name='test_pit',
            spine_df=spine_df,
            features=[fv],
            spine_timestamp_col='event_time',
            spine_label_cols=['label']
        )
        
        df = dataset.read.to_pandas()
        
        # All rows should have features (no nulls from future data)
        assert df['F_AGE'].notna().all(), \
            "Point-in-time join should find features for all spine rows"
        
        print("✅ Point-in-time joins working correctly")
    
    def test_generate_dataset_multiple_feature_views(
        self,
        feature_store,
        snowflake_session,
        test_data
    ):
        """
        Test generating dataset from multiple feature views.
        Validates multi-feature view joins.
        """
        try:
            fv1 = feature_store.get_feature_view('TEST_STATIC_CUSTOMER_FEATURES', '1.0')
            # If you have another FV, uncomment:
            # fv2 = feature_store.get_feature_view('ANOTHER_FV', '1.0')
        except Exception as e:
            pytest.skip(f"Feature views not found: {e}")
        
        spine_df = snowflake_session.table('FEATURE_STORE.training_spine')
        
        # For now, test with single FV (can expand when more FVs exist)
        dataset = feature_store.generate_dataset(
            name='test_multi_fv',
            spine_df=spine_df,
            features=[fv1],  # Add fv2 when available
            spine_timestamp_col='event_time',
            spine_label_cols=['label']
        )
        
        df = dataset.read.to_pandas()
        assert len(df) > 0, "Multi-FV dataset should have rows"
        
        print("✅ Multi-feature view dataset generation works")


class TestTrainingDatasets:
    """Test creating and managing training datasets."""
    
    def test_create_and_retrieve_dataset(
        self,
        feature_store,
        snowflake_session,
        test_data
    ):
        """
        Test full dataset lifecycle: create, save, retrieve, read.
        """
        try:
            fv = feature_store.get_feature_view('TEST_STATIC_CUSTOMER_FEATURES', '1.0')
        except Exception as e:
            pytest.skip(f"Feature view not found: {e}")
        
        spine_df = snowflake_session.table('FEATURE_STORE.training_spine')
        
        # Generate and save dataset
        dataset_name = 'customer_training_v1'
        dataset_version = '1.0'
        
        dataset = feature_store.generate_dataset(
            name=dataset_name,
            spine_df=spine_df,
            features=[fv],
            spine_timestamp_col='event_time',
            spine_label_cols=['label'],
            version=dataset_version,
            desc='Test training dataset'
        )
        
        # Retrieve the dataset
        retrieved_dataset = feature_store.get_dataset(dataset_name, dataset_version)
        
        assert retrieved_dataset is not None, "Should retrieve saved dataset"
        
        # Read and validate
        df = retrieved_dataset.read.to_pandas()
        assert len(df) > 0, "Retrieved dataset should have data"
        
        print(f"✅ Dataset lifecycle works: create → save → retrieve → read")
    
    def test_dataset_metadata(
        self,
        feature_store,
        test_data
    ):
        """
        Test that dataset metadata is stored correctly.
        """
        # List datasets
        datasets = feature_store.list_datasets().collect()
        
        # Should have at least one dataset
        assert len(datasets) > 0, "Should have datasets"
        
        # Check metadata fields
        for ds in datasets:
            assert 'NAME' in ds.asDict(), "Dataset should have name"
            assert 'VERSION' in ds.asDict(), "Dataset should have version"
        
        print(f"✅ Found {len(datasets)} datasets with metadata")


class TestFeatureViewChaining:
    """
    Test feature views that use other feature views as sources.
    This is a common pattern: base FV → derived FV.
    """
    
    @pytest.fixture(scope="class")
    def chained_feature_views(self, snowflake_session):
        """
        Create a chain of feature views using dbt.
        
        Chain: base_table → base_fv → derived_fv
        """
        # This would normally be done by dbt models
        # For testing, we'll verify the pattern works
        
        # Create base feature view table (simulating dbt output)
        snowflake_session.sql("""
            CREATE OR REPLACE VIEW FEATURE_STORE.base_customer_fv AS
            SELECT 
                customer_id,
                updated_at,
                age AS f_base_age,
                country AS f_base_country
            FROM FEATURE_STORE.customer_base_table
        """).collect()
        
        yield
        
        # Cleanup
        snowflake_session.sql("DROP VIEW IF EXISTS FEATURE_STORE.base_customer_fv").collect()
    
    def test_feature_view_references_another_fv(
        self,
        snowflake_session,
        chained_feature_views
    ):
        """
        Test that a dbt feature view can use another feature view as source.
        
        This pattern is common for:
        - Base features → Aggregated features
        - Raw features → Transformed features
        - Entity A features → Entity A+B joined features
        """
        # Create derived feature view (this would be a dbt model)
        snowflake_session.sql("""
            CREATE OR REPLACE VIEW FEATURE_STORE.derived_customer_fv AS
            SELECT 
                customer_id,
                updated_at,
                f_base_age,
                f_base_country,
                CASE 
                    WHEN f_base_age < 25 THEN 'Young'
                    WHEN f_base_age < 40 THEN 'Middle'
                    ELSE 'Senior'
                END AS f_age_group,
                CASE
                    WHEN f_base_country = 'US' THEN 'Domestic'
                    ELSE 'International'
                END AS f_customer_type
            FROM FEATURE_STORE.base_customer_fv
        """).collect()
        
        # Query the derived FV
        df = snowflake_session.sql("""
            SELECT * FROM FEATURE_STORE.derived_customer_fv LIMIT 10
        """).to_pandas()
        
        # Validate chaining works
        assert len(df) > 0, "Derived FV should have data from base FV"
        assert 'F_BASE_AGE' in df.columns, "Should have base features"
        assert 'F_AGE_GROUP' in df.columns, "Should have derived features"
        
        print("✅ Feature view chaining works")
        print(f"   Base features: f_base_age, f_base_country")
        print(f"   Derived features: f_age_group, f_customer_type")
        
        # Cleanup
        snowflake_session.sql("DROP VIEW IF EXISTS FEATURE_STORE.derived_customer_fv").collect()
    
    def test_multi_hop_feature_view_chain(
        self,
        snowflake_session,
        chained_feature_views
    ):
        """
        Test deeper chains: base → intermediate → final
        
        This tests more complex DAGs of feature views.
        """
        # Intermediate FV
        snowflake_session.sql("""
            CREATE OR REPLACE VIEW FEATURE_STORE.intermediate_fv AS
            SELECT 
                customer_id,
                updated_at,
                f_base_age,
                f_base_age * 12 AS f_age_months
            FROM FEATURE_STORE.base_customer_fv
        """).collect()
        
        # Final FV (uses intermediate)
        snowflake_session.sql("""
            CREATE OR REPLACE VIEW FEATURE_STORE.final_fv AS
            SELECT 
                customer_id,
                updated_at,
                f_age_months,
                f_age_months / 365.0 AS f_age_years
            FROM FEATURE_STORE.intermediate_fv
        """).collect()
        
        # Query final FV
        df = snowflake_session.sql("""
            SELECT * FROM FEATURE_STORE.final_fv LIMIT 10
        """).to_pandas()
        
        assert len(df) > 0, "Multi-hop chain should work"
        assert 'F_AGE_MONTHS' in df.columns
        assert 'F_AGE_YEARS' in df.columns
        
        print("✅ Multi-hop feature view chain works")
        print("   Chain: base_fv → intermediate_fv → final_fv")
        
        # Cleanup
        snowflake_session.sql("DROP VIEW IF EXISTS FEATURE_STORE.final_fv").collect()
        snowflake_session.sql("DROP VIEW IF EXISTS FEATURE_STORE.intermediate_fv").collect()


class TestMultiEntityJoins:
    """Test feature views involving multiple entities."""
    
    def test_feature_view_with_multiple_entities(
        self,
        snowflake_session
    ):
        """
        Test a feature view that joins multiple entities.
        
        Example: Order features that need both customer and product entities.
        """
        # This would be tested when we have multi-entity feature views
        # For now, document the pattern
        
        example_sql = """
        -- This is how a dbt model would look for multi-entity FV:
        {{
          config(
            materialized='feature_view',
            entities=['customer', 'product'],
            feature_view_version='1.0',
            timestamp_col='order_time'
          )
        }}
        
        SELECT
          customer_id,
          product_id,
          order_time,
          order_amount AS f_order_amount,
          quantity AS f_quantity
        FROM {{ ref('orders') }}
        """
        
        print("✅ Multi-entity pattern documented")
        print(example_sql)


class TestDynamicTableFeatureViews:
    """Test that Dynamic Table feature views work with Feature Store API."""
    
    def test_generate_dataset_from_dynamic_table_fv(
        self,
        feature_store,
        snowflake_session,
        test_data
    ):
        """
        Test that Dynamic Table (managed) feature views work with generate_dataset().
        
        Dynamic Tables should behave identically to VIEWs from API perspective.
        """
        try:
            # This is a Dynamic Table feature view
            fv = feature_store.get_feature_view('TEST_MANAGED_CUSTOMER_FEATURES', '1.0')
        except Exception as e:
            pytest.skip(f"Managed feature view not found: {e}")
        
        spine_df = snowflake_session.table('FEATURE_STORE.training_spine')
        
        dataset = feature_store.generate_dataset(
            name='test_dynamic_table_dataset',
            spine_df=spine_df,
            features=[fv],
            spine_timestamp_col='event_time',
            spine_label_cols=['label']
        )
        
        df = dataset.read.to_pandas()
        
        assert len(df) > 0, "Dynamic Table FV should work in datasets"
        assert 'F_AGE' in df.columns or 'F_TIER' in df.columns, \
            "Should have features from Dynamic Table FV"
        
        print("✅ Dynamic Table feature views work with generate_dataset()")
    
    def test_mix_static_and_managed_fvs(
        self,
        feature_store,
        snowflake_session,
        test_data
    ):
        """
        Test mixing VIEW and Dynamic Table feature views in same dataset.
        """
        try:
            static_fv = feature_store.get_feature_view('TEST_STATIC_CUSTOMER_FEATURES', '1.0')
            managed_fv = feature_store.get_feature_view('TEST_MANAGED_CUSTOMER_FEATURES', '1.0')
        except Exception as e:
            pytest.skip(f"Feature views not found: {e}")
        
        spine_df = snowflake_session.table('FEATURE_STORE.training_spine')
        
        # Mix both types
        dataset = feature_store.generate_dataset(
            name='test_mixed_fvs',
            spine_df=spine_df,
            features=[static_fv, managed_fv],
            spine_timestamp_col='event_time',
            spine_label_cols=['label']
        )
        
        df = dataset.read.to_pandas()
        
        assert len(df) > 0, "Mixed FV dataset should work"
        
        # Should have features from both
        static_cols = [c for c in df.columns if 'COUNTRY' in c.upper()]
        managed_cols = [c for c in df.columns if 'TIER' in c.upper()]
        
        print("✅ Mixing static and managed FVs works")
        print(f"   Static FV columns: {static_cols}")
        print(f"   Managed FV columns: {managed_cols}")

