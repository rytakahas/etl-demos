-- PLACEHOLDER: Fabric Warehouse SQL (T-SQL style)
-- Build Gold marts from Silver tables.

-- Example:
-- CREATE OR ALTER VIEW bank_gold.dim_customer AS
-- SELECT customer_id, segment
-- FROM bank_silver.stg_customers;

-- CREATE OR ALTER VIEW bank_gold.f_payment AS
-- SELECT payment_id, contract_id, payment_date, paid_amount, dpd_at_payment
-- FROM bank_silver.stg_payments;
