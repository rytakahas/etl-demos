{{ config(materialized='table') }}

with loans as (
  select
    dealer_id,
    application_date,
    loan_amount,
    ltv_ratio,
    loan_default
  from {{ ref('stg_corebank_loans') }}
),

agg as (
  select
    d.dealer_key,
    dd.date_key,
    count(*) as num_applications,
    -- treat non-defaults as "approvals"
    sum(case when loan_default = 0 then 1 else 0 end) as num_approvals,
    sum(loan_amount) as financed_amount,
    avg(ltv_ratio)   as avg_ltv,
    cast(null as numeric) as avg_rate
  from loans l
  left join {{ ref('dim_dealer') }} d
    on l.dealer_id = d.dealer_id
  left join {{ ref('dim_date') }} dd
    on l.application_date = dd.date
  group by
    d.dealer_key,
    dd.date_key
)

select * from agg

