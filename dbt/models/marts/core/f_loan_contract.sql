{{ config(materialized='table') }}

with loans as (
  select * from {{ ref('stg_corebank_loans') }}
),

joined as (
  select
    -- Natural key from Kaggle dataset
    l.loan_id,

    -- Dimension keys
    c.customer_key,
    p.product_key,
    d.dealer_key,
    dd.date_key as origination_date_key,

    -- Measures
    l.loan_amount        as amount_financed,
    l.ltv_ratio,
    l.asset_cost,
    l.primary_instal_amt,
    l.sec_instal_amt,

    -- Label (target)
    l.loan_default,

    -- Stub fields for bank-like schema
    cast(null as numeric) as interest_rate,
    cast(null as int64)   as term_months,
    cast(null as numeric) as down_payment,
    cast(null as numeric) as commission_amount,
    'INR'                 as currency_code,
    'IN'                  as country_code

  from loans l
  left join {{ ref('dim_customer') }} c
    on l.customer_id = c.customer_id
  left join {{ ref('dim_product') }} p
    on l.product_id  = p.product_id
  left join {{ ref('dim_dealer') }} d
    on l.dealer_id   = d.dealer_id
  left join {{ ref('dim_date') }} dd
    on l.application_date = dd.date
)

select * from joined