-- Risk-adjusted dealer profit: margin proxy minus credit losses (defaults - recoveries)
WITH contract_margin AS (
  SELECT
    c.dealer_key,
    c.country_entity_key,
    COUNT(DISTINCT c.customer_key) AS num_customers,
    SUM(c.approved_amount) AS total_volume,
    SUM(
      c.approved_amount
      * (c.interest_rate - c.funding_rate) / 100.0
      * (c.term_months / 12.0)
    ) AS est_lifetime_margin_eur
  FROM f_contract_retail c
  JOIN dim_date d
    ON d.date_key = c.orig_date_key
  WHERE d.full_date BETWEEN DATE '2025-01-01' AND DATE '2025-05-31'
  GROUP BY c.dealer_key, c.country_entity_key
),
defaults_agg AS (
  SELECT
    de.dealer_key,
    de.country_entity_key,
    SUM(de.default_amount - de.recovery_amount) AS credit_loss_eur
  FROM f_default_event de
  JOIN dim_date d
    ON d.date_key = de.default_date_key
  WHERE d.full_date BETWEEN DATE '2025-01-01' AND DATE '2025-05-31'
  GROUP BY de.dealer_key, de.country_entity_key
),
profit_by_dealer AS (
  SELECT
    m.dealer_key,
    m.country_entity_key,
    m.num_customers,
    m.total_volume,
    m.est_lifetime_margin_eur AS gross_margin_eur,
    COALESCE(d.credit_loss_eur, 0) AS credit_loss_eur,
    m.est_lifetime_margin_eur - COALESCE(d.credit_loss_eur, 0) AS net_profit_eur
  FROM contract_margin m
  LEFT JOIN defaults_agg d
    ON m.dealer_key = d.dealer_key
   AND m.country_entity_key = d.country_entity_key
)
SELECT
  dd.dealer_name,
  dd.city,
  ce.country_code,
  ce.country_name,
  p.num_customers,
  p.total_volume,
  p.gross_margin_eur,
  p.credit_loss_eur,
  p.net_profit_eur
FROM profit_by_dealer p
JOIN dim_dealer dd
  ON p.dealer_key = dd.dealer_key
JOIN dim_country_entity ce
  ON p.country_entity_key = ce.country_entity_key
ORDER BY p.net_profit_eur DESC;
