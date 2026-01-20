{{ config(materialized='view') }}

WITH base AS (
  SELECT
    cast(payment_key as int64) as payment_key,
    cast(contract_key as int64) as contract_key,
    cast(customer_key as int64) as customer_key,
    cast(dealer_key as int64) as dealer_key,
    cast(payment_date_key as int64) as payment_date_key,
    cast(country_entity_key as int64) as country_entity_key,
    cast(scheduled_amount as numeric) as scheduled_amount,
    cast(paid_amount as numeric) as paid_amount,
    cast(principal_component as numeric) as principal_component,
    cast(interest_component as numeric) as interest_component,
    cast(fee_component as numeric) as fee_component,
    cast(dpd_at_payment as int64) as dpd_at_payment
  FROM {{ source('raw', 'f_payment') }}
  WHERE payment_key IS NOT NULL
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      base.*,
      ROW_NUMBER() OVER (
        PARTITION BY payment_key
        ORDER BY payment_date_key DESC
      ) AS rn
    FROM base
  )
  WHERE rn = 1
)
SELECT * FROM dedup

