MATCH (cu:Customer {customerKey: $customerKey})<-[:hasCustomer]-(k:Contract)
OPTIONAL MATCH (e:DefaultEvent)-[:forContract]->(k)
OPTIONAL MATCH (p:Payment)-[:paymentForContract]->(k)
WITH cu, k,
     count(DISTINCT e) AS default_events,
     max(coalesce(p.dpdAtPayment, 0)) AS max_dpd,
     sum(CASE WHEN coalesce(p.dpdAtPayment,0) > 0 THEN 1 ELSE 0 END) AS late_payments
RETURN
  cu.customerKey AS customer,
  k.contractKey AS contract,
  default_events,
  max_dpd,
  late_payments
ORDER BY k.contractKey;
