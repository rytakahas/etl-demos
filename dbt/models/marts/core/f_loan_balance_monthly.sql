{{ config(materialized='view') }}

with base as (
  select
    loan_id                          as contract_key,
    origination_date_key             as calendar_month_key,
    amount_financed                  as opening_balance,
    cast(0 as numeric)               as principal_paid,
    cast(0 as numeric)               as interest_charged,
    amount_financed                  as closing_balance,
    cast(0 as int64)                 as days_past_due,
    'CURRENT'                        as arrears_bucket
  from {{ ref('f_loan_contract') }}
)

select * from base

