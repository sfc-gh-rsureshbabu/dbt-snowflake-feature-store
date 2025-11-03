{{
  config(
    materialized='entity',
    join_keys=['customer_id'],
    desc='Test customer entity for integration testing'
  )
}}

SELECT 1 WHERE FALSE


