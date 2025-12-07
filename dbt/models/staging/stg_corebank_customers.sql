{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw', 'loan_applications_raw') }}
),

deduped as (
  select
    cast(UniqueID as string) as customer_id,
    SAFE.PARSE_DATE('%d-%m-%y', cast(Date_of_Birth as string)) as date_of_birth,
    cast(branch_id as string) as branch_id,
    Employment_Type as employment_type,
    cast(State_ID as string) as state_id,
    cast(Current_pincode_ID as string) as current_pincode_id,
    'RETAIL' as customer_type,
    row_number() over (partition by cast(UniqueID as string) order by cast(UniqueID as string)) as rn
  from src
  where UniqueID is not null
)

select
  customer_id,
  date_of_birth,
  branch_id,
  employment_type,
  state_id,
  current_pincode_id,
  customer_type
from deduped
where rn = 1