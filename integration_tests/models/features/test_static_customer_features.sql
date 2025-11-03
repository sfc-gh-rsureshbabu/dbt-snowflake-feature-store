{{
  config(
    materialized='feature_view',
    entities=['test_customer_entity'],
    feature_view_version='1.0',
    timestamp_col='updated_at',
    desc='Customer features for testing'
  )
}}

SELECT
  1 AS customer_id,
  CURRENT_TIMESTAMP() AS updated_at,
  25 AS f_age,
  'US' AS f_country

