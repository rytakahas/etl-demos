MATCH (d:Dealer)<-[:hasDealer]-(k:Contract)-[:inCountry]->(co:Country)
OPTIONAL MATCH (k)-[:hasVehicle]->(v:Vehicle)
OPTIONAL MATCH (e:DefaultEvent)-[:forContract]->(k)
WITH d, co, v,
     count(DISTINCT k) AS approvals,
     count(DISTINCT e) AS default_events,
     sum(coalesce(e.defaultAmount,0.0)) AS default_amount,
     sum(coalesce(e.recoveryAmount,0.0)) AS recovery_amount
RETURN
  d.dealerKey AS dealer,
  coalesce(co.countryCode, co.countryKey, "UNK") AS country,
  coalesce(v.fuelType, "unknown") AS fuel_type,
  approvals,
  default_events,
  default_amount,
  recovery_amount,
  CASE WHEN default_amount = 0 THEN 0 ELSE round(recovery_amount / default_amount, 4) END AS recovery_rate
ORDER BY default_amount DESC, approvals DESC;
