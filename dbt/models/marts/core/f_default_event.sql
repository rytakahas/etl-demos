{{ config(materialized='table') }}

with defaults as (
  select
    l.loan_id,
    l.application_date,
    l.loan_amount,
    l.loan_default
  from {{ ref('stg_corebank_loans') }} l
  where l.loan_default = 1
),
dim_dates as (
  select
    date_key,
    date as calendar_date
  from {{ ref('dim_date') }}
),

joined as (
  select
    d.loan_id                                as contract_key,
    dd.date_key                              as default_date_key,
    d.loan_amount                            as default_balance,
    d.loan_amount                            as writeoff_amount,
    cast(0 as numeric)                       as recovery_amount,
    cast(0 as numeric)                       as recovery_rate
  from defaults d
  left join dim_dates dd
    on d.application_date = dd.calendar_date
)

select
  row_number() over (order by contract_key) as default_event_id,
  *
from joined

