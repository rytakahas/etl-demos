-- Dealer profit estimate (proxy): approximate lifetime margin if est_lifetime_margin_eur is not available
-- est_margin = approved_amount * (interest_rate - funding_rate)/100 * (term_months/12)
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
)
SELECT
  dd.dealer_name,
  dd.city,
  ce.country_code,
  ce.country_name,
  cm.num_customers,
  cm.total_volume,
  cm.est_lifetime_margin_eur AS net_profit_estimate
FROM contract_margin cm
JOIN dim_dealer dd
  ON cm.dealer_key = dd.dealer_key
JOIN dim_country_entity ce
  ON cm.country_entity_key = ce.country_entity_key
ORDER BY cm.est_lifetime_margin_eur DESC;
