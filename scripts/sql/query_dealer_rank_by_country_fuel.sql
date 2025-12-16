-- Dealer ranking per country & fuel type (EV / Hybrid / ICE) using window functions
WITH contract_margin AS (
  SELECT
    c.dealer_key,
    c.country_entity_key,
    v.fuel_type,
    COUNT(DISTINCT c.customer_key) AS num_customers,
    SUM(c.approved_amount) AS total_volume,
    SUM(
      c.approved_amount
      * (c.interest_rate - c.funding_rate) / 100.0
      * (c.term_months / 12.0)
    ) AS est_lifetime_margin_eur
  FROM f_contract_retail c
  JOIN dim_date d ON d.date_key = c.orig_date_key
  JOIN dim_vehicle v ON v.vehicle_key = c.vehicle_key
  WHERE d.full_date BETWEEN DATE '2025-01-01' AND DATE '2025-05-31'
  GROUP BY c.dealer_key, c.country_entity_key, v.fuel_type
),
defaults_agg AS (
  SELECT
    de.dealer_key,
    c.country_entity_key,
    v.fuel_type,
    SUM(de.default_amount - de.recovery_amount) AS credit_loss_eur
  FROM f_default_event de
  JOIN f_contract_retail c ON c.contract_key = de.contract_key
  JOIN dim_vehicle v ON v.vehicle_key = c.vehicle_key
  JOIN dim_date d ON d.date_key = de.default_date_key
  WHERE d.full_date BETWEEN DATE '2025-01-01' AND DATE '2025-05-31'
  GROUP BY de.dealer_key, c.country_entity_key, v.fuel_type
),
profit_by_dealer AS (
  SELECT
    m.dealer_key,
    m.country_entity_key,
    m.fuel_type,
    m.num_customers,
    m.total_volume,
    COALESCE(d.credit_loss_eur, 0) AS credit_loss_eur,
    m.est_lifetime_margin_eur - COALESCE(d.credit_loss_eur, 0) AS net_profit_eur
  FROM contract_margin m
  LEFT JOIN defaults_agg d
    ON m.dealer_key = d.dealer_key
   AND m.country_entity_key = d.country_entity_key
   AND m.fuel_type = d.fuel_type
),
ranked AS (
  SELECT
    ce.country_code,
    ce.country_name,
    p.fuel_type,
    dd.dealer_name,
    dd.city,
    p.num_customers,
    p.total_volume,
    p.net_profit_eur,
    RANK() OVER (
      PARTITION BY ce.country_code, p.fuel_type
      ORDER BY p.net_profit_eur DESC
    ) AS rank_in_country_fuel,
    p.net_profit_eur / NULLIF(
      SUM(p.net_profit_eur) OVER (PARTITION BY ce.country_code, p.fuel_type),
      0
    ) AS profit_share_in_segment
  FROM profit_by_dealer p
  JOIN dim_dealer dd ON p.dealer_key = dd.dealer_key
  JOIN dim_country_entity ce ON p.country_entity_key = ce.country_entity_key
)
SELECT *
FROM ranked
WHERE fuel_type IN ('EV','Hybrid','ICE')
ORDER BY country_code, fuel_type, rank_in_country_fuel;
