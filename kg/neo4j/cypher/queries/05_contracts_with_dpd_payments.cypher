// Input: $days (default 90)
WITH date() AS today, coalesce($days, 90) AS days
MATCH (p:Payment)-[:FOR_CONTRACT]->(k:Contract)
WHERE p.paymentDate IS NOT NULL
  AND p.paymentDate >= (today - duration('P' + toString(days) + 'D'))
  AND coalesce(p.dpdAtPayment, 0) > 0
RETURN k.contractKey AS contract,
       max(p.dpdAtPayment) AS max_dpd,
       count(p) AS late_payments
ORDER BY max_dpd DESC, late_payments DESC
LIMIT 100;

