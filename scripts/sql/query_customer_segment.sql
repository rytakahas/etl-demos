-- Customer-centric analytics (by customer segment)
SELECT
  cust.segment,
  COUNT(DISTINCT c.contract_key) AS num_contracts,
  SUM(c.approved_amount) AS total_volume
FROM f_contract_retail c
JOIN dim_customer cust
  ON c.customer_key = cust.customer_key
GROUP BY cust.segment;
