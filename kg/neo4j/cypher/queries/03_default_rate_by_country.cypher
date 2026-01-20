MATCH (country:Country)<-[:inCountry]-(k:Contract)
OPTIONAL MATCH (e:DefaultEvent)-[:forContract]->(k)
WITH country, k, count(e) AS e_cnt
WITH country,
     count(k) AS total_contracts,
     sum(CASE WHEN e_cnt > 0 THEN 1 ELSE 0 END) AS defaulted_contracts
RETURN
  country.countryKey AS country,
  total_contracts,
  defaulted_contracts,
  round(1.0 * defaulted_contracts / total_contracts, 4) AS default_rate
ORDER BY default_rate DESC;

