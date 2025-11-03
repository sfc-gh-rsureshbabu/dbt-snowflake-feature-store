{{
  config(
    materialized='feature_view',
    entities=['test_customer_entity'],
    feature_view_version='1.0',
    timestamp_col='updated_at',
    refresh_freq='1 minute',
    warehouse='ADMIN_WH',
    desc='Managed customer features (DYNAMIC TABLE)'
  )
}}

-- Dynamic Tables require at least one base table (not a view)
SELECT
  customer_id,
  updated_at,
  age AS f_age,
  tier AS f_tier
FROM rsureshbabu.FEATURE_STORE.customer_base_table

