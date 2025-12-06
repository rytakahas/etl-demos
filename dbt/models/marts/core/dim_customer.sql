{{ config(materialized='table') }}

with base as (
  select
    customer_id,
    date_of_birth,
    employment_type,
    state_id,
    current_pincode_id
  from {{ ref('stg_corebank_customers') }}
),

enriched as (
  select
    customer_id,
    date_of_birth,
    employment_type,
    state_id,
    current_pincode_id,
    case
      when date_of_birth is null then 'unknown'
      when date_diff(current_date(), date_of_birth, year) < 30 then '18-29'
      when date_diff(current_date(), date_of_birth, year) < 40 then '30-39'
      when date_diff(current_date(), date_of_birth, year) < 50 then '40-49'
      else '50+'
    end as age_band
  from base
),

with_keys as (
  select
    row_number() over (order by customer_id) as customer_key,
    customer_id,
    'individual' as customer_type,
    null        as kyc_segment,
    age_band,
    null        as region,
    'IN'        as country,
    null        as language,
    null        as income_band,
    employment_type as occupation
  from enriched
)

select * from with_keys

