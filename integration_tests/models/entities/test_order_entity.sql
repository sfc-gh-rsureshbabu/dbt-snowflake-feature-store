{{
  config(
    materialized='entity',
    join_keys=['order_id'],
    desc='Test order entity'
  )
}}

SELECT 1 WHERE FALSE


