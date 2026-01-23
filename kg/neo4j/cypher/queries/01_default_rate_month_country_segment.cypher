MATCH (k:Contract)-[:inCountry]->(co:Country)
MATCH (k)-[:hasCustomer]->(cu:Customer)
OPTIONAL MATCH (e:DefaultEvent)-[:forContract]->(k)
WITH
  coalesce(co.countryCode, co.countryKey, "UNK") AS country,
  coalesce(cu.segment, "unknown") AS segment,
  k,
  max(date(e.eventDate)) AS d,
  count(e) AS e_cnt
WITH country, segment,
     CASE WHEN d IS NULL THEN NULL ELSE date({year:d.year, month:d.month, day:1}) END AS month,
     k, e_cnt
WITH country, segment, month,
     count(DISTINCT k) AS total_contracts,
     count(DISTINCT CASE WHEN e_cnt > 0 THEN k END) AS defaulted_contracts
RETURN
  month, country, segment,
  total_contracts, defaulted_contracts,
  CASE WHEN total_contracts=0 THEN 0 ELSE round(1.0 * defaulted_contracts / total_contracts, 4) END AS default_rate
ORDER BY month DESC, country, segment;
