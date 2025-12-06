{{ config(materialized='table') }}

select
  row_number() over (order by status_code) as status_key,
  status_code,
  case
    when status_code in ('ACTIVE', 'CLOSED') then 'PERFORMING'
    else 'NON_PERFORMING'
  end as status_group
from (
  select 'ACTIVE' as status_code union all
  select 'CLOSED' union all
  select 'DEFAULTED' union all
  select 'WRITTEN_OFF'
)
