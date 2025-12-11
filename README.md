# Bank DWH Demo – Domain-Driven, Layered Warehouse on BigQuery

This repository is a **mini style bank DWH** demo:

- Domain-driven design: Customers, Products, Dealers, Loans, Payments, Defaults.
- Layered architecture: **Bronze (raw)** → **Silver (staging)** → **Gold (marts)**.
- Implemented with **BigQuery + dbt + Airflow + Terraform**, but cloud-agnostic.
- Includes **small synthetic CSV datasets** for local testing on a laptop.

## 1. Datasets

Under `data/`:

- `customers.csv` → raw customers (`customers_raw`)
- `auto_loan_default.csv` → raw loans (`loan_applications_raw`)
- `payments.csv` → raw payments (`payments_raw`)

Each file has a header row and a few thousand records (small enough for a MacBook,
but realistic enough to test the whole pipeline).

## 2. How to run end-to-end

### 2.1. Load raw CSVs into BigQuery

Example using the `bq` CLI (adjust project/dataset names):

```bash
bq load   --location=EU   --source_format=CSV   --skip_leading_rows=1   "your-raw-project:ryoji_raw_demos.customers_raw"   data/customers.csv

bq load   --location=EU   --source_format=CSV   --skip_leading_rows=1   "your-raw-project:ryoji_raw_demos.loan_applications_raw"   data/auto_loan_default.csv

bq load   --location=EU   --source_format=CSV   --skip_leading_rows=1   "your-raw-project:ryoji_raw_demos.payments_raw"   data/payments.csv
```

Or use the Python script:

```bash
python scripts/load_raw_to_bq.py --project your-raw-project --dataset ryoji_raw_demos --table customers_raw          --csv data/customers.csv
python scripts/load_raw_to_bq.py --project your-raw-project --dataset ryoji_raw_demos --table loan_applications_raw --csv data/auto_loan_default.csv
python scripts/load_raw_to_bq.py --project your-raw-project --dataset ryoji_raw_demos --table payments_raw          --csv data/payments.csv
```

### 2.2. Run dbt (staging + marts)

```bash
cd dbt
dbt run  --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
```

This builds:

- Staging: `stg_corebank_loans`, `stg_corebank_customers`, `stg_payments`
- Dimensions: `dim_customer`, `dim_product`, `dim_dealer`, `dim_date`, `dim_contract_status`
- Facts: `f_loan_contract`, `f_loan_balance_monthly`, `f_payment_transaction`, `f_default_event`, `f_dealer_performance_daily`

### 2.3. Optional: Orchestrate with Airflow

The `dags/bank_etl_dag.py` DAG shows how to:

- Load raw CSVs (Bronze)
- Run dbt models (Silver+Gold)
- Run dbt tests

This can run on local Airflow, Cloud Composer, or MWAA with small changes.


## Metadata-driven ingestion & generic staging

To keep this demo closer to an “enterprise-ready” setup without making it heavy, the repo uses two patterns:

### 1. Metadata-driven ingestion (Airflow)

Raw CSV → BigQuery ingestion is configured in a single file:

- `config/raw_sources.yml`

Example:

```yaml
raw_sources:
  - name: customers_raw
    project_id: vivid-layout-453307-p4
    dataset_id: ryoji_raw_demos
    table_id: customers_raw
    csv_path: data/customers.csv
  - name: loan_applications_raw
    project_id: vivid-layout-453307-p4
    dataset_id: ryoji_raw_demos
    table_id: loan_applications_raw
    csv_path: data/vehicle_loans_train_clean.csv
  - name: payments_raw
    project_id: vivid-layout-453307-p4
    dataset_id: ryoji_raw_demos
    table_id: payments_raw
    csv_path: data/payments.csv
