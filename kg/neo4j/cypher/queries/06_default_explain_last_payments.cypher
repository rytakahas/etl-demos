// Input: $contractKey
MATCH (k:Contract {contractKey: $contractKey})
MATCH (e:DefaultEvent)-[:forContract]->(k)
WITH k, e
MATCH (p:Payment)-[:FOR_CONTRACT]->(k)
WHERE p.paymentDate IS NOT NULL AND e.eventDate IS NOT NULL AND p.paymentDate <= e.eventDate
WITH k, e, p
ORDER BY p.paymentDate DESC
RETURN
  k.contractKey AS contract,
  e.eventDate AS default_date,
  collect({paymentDate:p.paymentDate, paid:p.paidAmount, scheduled:p.scheduledAmount, dpd:p.dpdAtPayment})[0..5] AS last_5_payments;

