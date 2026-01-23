{{ config(materialized='table') }}

with loans as (
  select * from {{ ref('stg_corebank_loans') }}
),

joined as (
  select
    l.loan_id,

    c.customer_key,
    p.product_key,
    d.dealer_key,
    dd.date_key as origination_date_key,

    -- DEMO mapping (synthetic): map loan_id -> vehicle_key 1..10
    cast(mod(coalesce(safe_cast(l.loan_id as int64), 0), 10) + 1 as int64) as vehicle_key,

    -- DEMO mapping (synthetic): map all to DE (1) unless you have real country in source
    cast(1 as int64) as country_entity_key,

    l.loan_amount        as amount_financed,
    l.ltv_ratio,
    l.asset_cost,
    l.primary_instal_amt,
    l.sec_instal_amt,

    l.loan_default,

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
