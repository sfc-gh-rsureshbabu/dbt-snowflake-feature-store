{{
  config(
    materialized='feature_view',
    entities=['test_customer_entity'],
    feature_view_version='1.0',
    timestamp_col='updated_at',
    desc='Base customer features - source for derived features'
  )
}}

-- Base features directly from source table
SELECT
  customer_id,
  updated_at,
  age AS f_base_age,
  country AS f_base_country,
  tier AS f_base_tier
FROM {{ source('test_sources', 'customer_base_table') }}

