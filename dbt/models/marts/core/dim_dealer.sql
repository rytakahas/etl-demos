{{ config(materialized='table') }}

with src as (
  select distinct
    dealer_id
  from {{ ref('stg_corebank_loans') }}
  where dealer_id is not null
),

with_keys as (
  select
    row_number() over (order by dealer_id) as dealer_key,
    dealer_id,
    dealer_id as dealer_name,
    'IN' as country,           -- Kaggle dataset is India loans
    null as region,
    null as dealer_segment,
    'offline' as channel
  from src
)

select * from with_keys

