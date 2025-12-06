{{ config(materialized='view') }}

select
  cast(loan_id as string)                                   as contract_key,
  cast(format_date('%Y%m%d', payment_date) as int64)        as payment_date_key,
  1                                                         as channel_key,
  cast(amount as numeric)                                   as amount,
  cast(amount as numeric)                                   as principal_component,
  cast(0 as numeric)                                        as interest_component,
  cast(0 as numeric)                                        as fee_component,
  cast(0 as numeric)                                        as late_fee_component
from {{ ref('stg_payments') }}

