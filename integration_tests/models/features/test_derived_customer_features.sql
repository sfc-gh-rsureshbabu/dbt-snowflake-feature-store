{{
  config(
    materialized='feature_view',
    entities=['test_customer_entity'],
    feature_view_version='1.0',
    timestamp_col='updated_at',
    desc='Derived customer features - uses base features as source'
  )
}}

-- Derived features that transform base features
-- This demonstrates feature view chaining: base_fv → derived_fv
SELECT
  customer_id,
  updated_at,
  
  -- Include base features
  f_base_age,
  f_base_country,
  
  -- Derive new features from base
  CASE 
    WHEN f_base_age < 25 THEN 'Young'
    WHEN f_base_age < 40 THEN 'Middle'
    WHEN f_base_age < 60 THEN 'Senior'
    ELSE 'Elder'
  END AS f_age_group,
  
  CASE
    WHEN f_base_country = 'US' THEN 'Domestic'
    ELSE 'International'
  END AS f_customer_segment,
  
  f_base_age * 12 AS f_age_in_months

FROM {{ ref('test_base_customer_features') }}

