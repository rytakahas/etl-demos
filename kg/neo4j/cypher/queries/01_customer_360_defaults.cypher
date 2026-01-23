// Input: $customerKey
MATCH (c:Customer {customerKey: $customerKey})<-[:hasCustomer]-(k:Contract)
OPTIONAL MATCH (e:DefaultEvent)-[:forContract]->(k)
RETURN
  c.customerKey AS customer,
  k.contractKey AS contract,
  count(e) AS default_event_count,
  collect(DISTINCT e)[0..10] AS sample_default_events
ORDER BY k.contractKey;

