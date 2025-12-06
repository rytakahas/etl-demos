{{ config(materialized='table') }}

with dates as (
  select
    cast(date_day as date) as date
  from unnest(generate_date_array('2015-01-01', '2030-12-31')) as date_day
)
select
  cast(format_date('%Y%m%d', date) as int64) as date_key,
  date,
  extract(year from date) as year,
  extract(quarter from date) as quarter,
  extract(month from date) as month,
  extract(week from date) as week,
  extract(dayofweek from date) as day_of_week,
  case when extract(dayofweek from date) in (1,7) then true else false end as is_weekend
from dates
