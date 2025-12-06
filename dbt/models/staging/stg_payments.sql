{{ config(materialized='view') }}

select
  cast(loan_id as string)        as loan_id,
  cast(payment_date as date)     as payment_date,
  cast(amount as numeric)        as amount,
  cast(principal_amt as numeric) as principal_amt,
  cast(interest_amt as numeric)  as interest_amt,
  cast(fee_amt as numeric)       as fee_amt,
  cast(late_fee_amt as numeric)  as late_fee_amt,
  cast(channel_id as int64)      as channel_id
from {{ source('raw', 'payments_raw') }}
