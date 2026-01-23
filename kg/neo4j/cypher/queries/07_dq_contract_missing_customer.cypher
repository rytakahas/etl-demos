MATCH (k:Contract)
WHERE NOT (k)-[:hasCustomer]->(:Customer)
RETURN count(k) AS contracts_missing_customer;

