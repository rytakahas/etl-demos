{{ config(materialized='table') }}

with src as (
  select distinct
    cast(product_id as string) as product_id
  from {{ ref('stg_corebank_loans') }}
),

with_keys as (
  select
    row_number() over (order by product_id) as product_key,
    product_id,
    'vehicle_loan'  as product_type,
    null            as interest_scheme,
    null            as term_months,
    null            as brand,
    null            as program
  from src
)

select * from with_keys

