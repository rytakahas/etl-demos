{{ config(materialized='view') }}

select
  cast(default_event_key as int64) as default_event_key,
  cast(contract_key as int64) as contract_key,
  cast(customer_key as int64) as customer_key,
  cast(dealer_key as int64) as dealer_key,
  cast(default_date_key as int64) as default_date_key,
  cast(country_entity_key as int64) as country_entity_key,
  cast(default_amount as numeric) as default_amount,
  cast(recovery_amount as numeric) as recovery_amount,
  cast(lgd_pct as numeric) as lgd_pct
from {{ source('raw', 'f_default_event') }}
where default_event_key is not null
