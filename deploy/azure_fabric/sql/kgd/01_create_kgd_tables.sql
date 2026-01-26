-- PLACEHOLDER: Create/refresh KGD tables from Gold.
-- Node tables: one row per node
-- Edge tables: one row per relationship

-- Example nodes:
-- CREATE OR ALTER VIEW bank_gold.kg_node_customer AS
-- SELECT customer_id, segment
-- FROM bank_gold.dim_customer;

-- CREATE OR ALTER VIEW bank_gold.kg_node_contract AS
-- SELECT contract_id, term_months, approved_amount, interest_rate, country_entity_key
-- FROM bank_gold.f_contract_retail;

-- Example edges:
-- CREATE OR ALTER VIEW bank_gold.kg_edge_contract_has_customer AS
-- SELECT contract_id, customer_id
-- FROM bank_gold.f_contract_retail;

-- CREATE OR ALTER VIEW bank_gold.kg_edge_contract_has_payment AS
-- SELECT contract_id, payment_id
-- FROM bank_gold.f_payment;
