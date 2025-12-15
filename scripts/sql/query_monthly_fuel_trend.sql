-- Monthly EV vs ICE vs Hybrid trend per country using window functions
WITH contracts_month AS (
  SELECT
    ce.country_code,
    v.fuel_type,
    DATE_TRUNC(d.full_date, MONTH) AS month_start,
    SUM(c.approved_amount) AS total_volume_eur
  FROM f_contract_retail c
  JOIN dim_date d ON d.date_key = c.orig_date_key
  JOIN dim_country_entity ce ON ce.country_entity_key = c.country_entity_key
  JOIN dim_vehicle v ON v.vehicle_key = c.vehicle_key
  WHERE d.full_date BETWEEN DATE '2025-01-01' AND DATE '2025-05-31'
  GROUP BY ce.country_code, v.fuel_type, DATE_TRUNC(d.full_date, MONTH)
),
with_share AS (
  SELECT
    country_code,
    fuel_type,
    month_start,
    total_volume_eur,
    total_volume_eur / NULLIF(
      SUM(total_volume_eur) OVER (PARTITION BY country_code, month_start),
      0
    ) AS volume_share_in_month
  FROM contracts_month
)
SELECT *
FROM with_share
WHERE fuel_type IN ('EV','Hybrid','ICE')
ORDER BY country_code, month_start, fuel_type;
