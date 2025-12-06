{{ config(materialized='view') }}

with base as (
  select
    customer_id,
    date_of_birth_raw      as date_of_birth,
    employment_type,
    state_id,
    current_pincode_id
  from {{ ref('stg_corebank_loans') }}
),

dedup as (
  select
    customer_id,
    any_value(date_of_birth)      as date_of_birth,
    any_value(employment_type)    as employment_type,
    any_value(state_id)           as state_id,
    any_value(current_pincode_id) as current_pincode_id
  from base
  group by customer_id
)

select * from dedup

