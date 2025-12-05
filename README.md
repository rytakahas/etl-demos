# Bank DWH Demo – Domain-Driven, Layered Warehouse on BigQuery

This repository contains a **Bank–style** data warehouse and ETL/ELT pipeline,
implemented on **Google BigQuery** but designed to be **cloud-agnostic** and easily
portable to **AWS (S3 + Glue + Athena/Redshift)** or **Azure**.

The goal is to demonstrate:

- A **domain-driven warehouse** for an auto-finance / banking context.
- A **layered architecture** (Bronze / Silver / Gold) for heterogeneous sources
  (CSV, XLSX, APIs).
- A **dimensional model** (Kimball style) that supports Finance, Risk and Sales,
  while retaining a single source of truth.
- How this warehouse can be exposed to **BI tools** (Power BI / Qlik) and internal
  **web applications / APIs**.

---

## 1. Business context

Imagine **Bank**, financing:

- New and used **cars and motorcycles** via dealers.
- Customers in multiple European countries.
- With cooperation partners (partner banks, local finance entities).

The bank needs a warehouse that can:

- Integrate data from **core banking systems**, **dealer CRMs**, **partner banks**,
  and **manual Excel/CSV uploads**.
- Provide **stable, well-governed datasets** for:
  - **Finance** (P&L, interest income, fees, provisions)
  - **Risk** (PD/LGD/EAD, default events, arrears, collections)
  - **Sales / Dealer Management** (volumes, conversions, dealer performance)
- Serve **BI dashboards** and **internal web tools** (credit officer UI,
  dealer management UI, etc.).

---

## 2. Architecture overview

The overall architecture uses **layered zones** and **domain-driven modeling**.

### 2.1. Layers (Bronze / Silver / Gold)

**Bronze – Raw / Landing**

- Direct copies of source data:
  - CSV / XLSX from finance & dealers.
  - Core banking extracts (loans, payments, customers).
  - Partner bank feeds / API dumps.
- Stored with minimal transformation:
  - Add metadata: `source_system`, `loaded_at`, `file_name`, etc.
  - No heavy business logic.
- Purpose: **ground-truth archive** & reproducibility.

**Silver – Standardized / Staging**

- Cleaned and standardized tables:
  - Uniform types (dates, numerics, strings).
  - Canonical IDs (e.g. `customer_id`, `loan_id`, `dealer_id`, `product_id`).
  - Consistent codes for country, currency, product type, channels, etc.
- Implemented via **dbt** or **PySpark**:
  - `stg_corebank_loans`, `stg_corebank_customers`
  - `stg_dealer_sales`
  - `stg_partner_bank_flows`
- Purpose: **one clean, consistent view per domain**.

**Gold – Integrated Warehouse / Marts**

- **Dimensional models** (Kimball style):
  - **Dimensions** (who/what/where/when/how)
  - **Facts** (business events & measures)
- Multiple **marts** by domain, but with **conformed dimensions**:
  - Risk mart, Finance mart, Sales/Dealer mart
  - All share `dim_customer`, `dim_product`, `dim_dealer`, `dim_date`, etc.
- Purpose: **fast, easy analytics & reporting** with a single source of truth.

---

## 3. Domain-driven design

Think in terms of **business domains**, not technical tables:

- **Customer & Party** – customer, dealer, partner bank
- **Products** – loan products, interest schemes, insurance add-ons
- **Contracts & Accounts** – loans, leases, credit lines
- **Transactions & Payments** – installments, interest, fees, penalties
- **Collections & Risk** – arrears, defaults, risk scores
- **Reference & Master Data** – country, currency, calendar, channels

### 3.1. Core layer vs marts

Within the warehouse:

- **Core layer**: normalized / lightly dimensional tables shared across domains.
  - `dim_customer`, `dim_product`, `dim_dealer`, `dim_date`, `dim_contract_status`, etc.
- **Marts**: star schemas optimized for specific use cases:
  - **Risk mart** – `f_loan_exposure`, `f_default_event`
  - **Finance mart** – `f_interest_income`, `f_fee_income`, `f_provision`
  - **Dealer mart** – `f_dealer_sales`, `f_dealer_commissions`

All marts **reuse the same dimensions** so that:

- Risk sees PD/LGD/EAD per customer/product/date.
- Finance sees P&L, net interest margin (NIM), fee income, provisions.
- Sales sees application volumes, approvals, financed amounts, dealer KPIs.

And everyone can relate their metrics on the same axes: customer, product, dealer, time.

---

## 4. Dimensional model (Bank mini-schema)

### 4.1. ER diagram

The high-level ER diagram for the demo looks like this:

> **[ER diagram – Bank DWH](bank_er.png)**

(Replace with the actual PNG if you add it to the repo.)

### 4.2. Core dimensions

#### `dim_customer`

Describes **who** is receiving finance (person or company).

- `customer_key` (surrogate key, INT)
- `customer_id` (business key from core banking)
- `customer_type` (individual / company)
- `kyc_segment` / risk segment
- `age_band`
- `region`, `country`, `language`
- `income_band`, `occupation`
- Optional: flags like `pep_flag`, `sanction_flag` (for regulated contexts).

#### `dim_product`

Describes **what** is being sold (loan / lease / add-on).

- `product_key`
- `product_id` (loan product code)
- `product_type` (car loan, bike loan, personal loan, lease)
- `interest_scheme` (fixed / variable)
- `term_months`
- `brand` (car, motorcycle, other brand)
- `program` (promo campaign, partner program, etc.)

#### `dim_dealer`

Describes **where** the loan is originated (dealer / channel).

- `dealer_key`
- `dealer_id`
- `dealer_name`
- `country`, `region`
- `dealer_segment` (volume tier, e.g. A/B/C)
- `channel` (online / offline / hybrid)

#### `dim_date` (calendar)

Describes the **when**.

- `date_key` (YYYYMMDD, INT)
- `date`
- `year`, `quarter`, `month`, `week`
- `day_of_week`
- `is_weekend`, `is_holiday` (per country)

#### `dim_contract_status`

Describes lifecycle status of a contract.

- `status_key`
- `status_code` (`active`, `closed`, `defaulted`, `written_off`, etc.)
- Optional: `status_group` (`performing`, `non-performing`).

---

### 4.3. Core facts

#### `f_loan_contract`

**Grain:** one row per **loan / lease contract**.

**Keys:**

- `contract_key` (optional surrogate)
- `customer_key`
- `product_key`
- `dealer_key`
- `origination_date_key` (FK to `dim_date`)
- `status_key` (FK to `dim_contract_status`)

**Measures / attributes:**

- `amount_financed`
- `interest_rate`
- `term_months`
- `ltv_ratio` (loan-to-value)
- `down_payment`
- `commission_amount` (dealer commission)
- `currency_code`
- `country_code`

Use cases:

- Portfolio overview by country, product, dealer.
- Volumes of new business, average LTV, average interest rate.

---

#### `f_loan_balance_monthly`

**Grain:** one row per **loan per month**.

**Keys:**

- `contract_key`
- `calendar_month_key` (FK to `dim_date`)

**Measures:**

- `opening_balance`
- `principal_paid`
- `interest_charged`
- `fees_charged`
- `closing_balance`
- `days_past_due` (max in the month)
- `arrears_bucket` (current, 30dpd, 60dpd, 90dpd+)

Use cases:

- Exposure at Default (EAD).
- Aging analysis, arrears tracking.
- Historical views for provisioning and IFRS9/CECL modelling inputs.

---

#### `f_payment_transaction`

**Grain:** one row per **payment event**.

**Keys:**

- `contract_key`
- `payment_date_key` (FK to `dim_date`)
- `channel_key` (if you define a `dim_channel`)

**Measures:**

- `amount`
- `principal_component`
- `interest_component`
- `fee_component`
- `late_fee_component` (if applicable)

Use cases:

- Cash-flow analysis.
- Behavioural analytics: early payments, partial payments, missed payments.
- Feeding ML models for customer behaviour and collections.

---

#### `f_default_event`

**Grain:** one row per **default / write-off event** (if any).

**Keys:**

- `contract_key`
- `default_date_key` (FK to `dim_date`)

**Measures:**

- `default_balance` (exposure at default)
- `writeoff_amount`
- `recovery_amount`
- `recovery_rate`

Use cases:

- LGD modelling & back-testing.
- Portfolio default statistics by product, dealer, country, vintage.

---

#### `f_dealer_performance_daily`

**Grain:** one row per **dealer per day**.

**Keys:**

- `dealer_key`
- `date_key`

**Measures:**

- `num_applications`
- `num_approvals`
- `financed_amount`
- `avg_ltv`
- `avg_rate`

Use cases:

- Dealer performance dashboards.
- Sales incentives, dealer scorecards.
- Detect under- or over-performing dealers and channels.

---

## 5. Handling multiple source types (CSV, XLSX, APIs)

In the ETL/ELT pipeline:

### 5.1. Ingestion (Bronze)

For each source:

- `ingest_corebank_raw`
  - Loads core banking extracts (loans, customers, payments) into `*_raw` tables.
- `ingest_dealer_excel_raw`
  - Parses XLSX uploads from dealers and loads them into `dealer_sales_raw`.
- `ingest_partner_bank_api_raw`
  - Calls partner bank APIs, lands data as JSON-like tables.

All raw tables live in a **raw dataset** (e.g. `ryoji_raw_demos`) and only have
minimal metadata transformations.

### 5.2. Standardization (Silver)

Using **dbt** (or PySpark), we create standardized staging models:

- `stg_corebank_loans`
- `stg_corebank_customers`
- `stg_payments`
- `stg_dealer_sales`
- `stg_partner_bank_flows`

Typical transformations:

- Clean and standardize date formats.
- Normalize IDs (add prefixes or map local keys to global ones).
- Map local product codes to a unified `product_id`.
- Clean country and currency codes.
- Apply basic data-quality checks (tests in dbt).

### 5.3. Integration (Gold)

Staging outputs are then integrated into:

- `dim_customer` (combining core banking + KYC system).
- `dim_product` (combining product catalog + promo programs).
- `dim_dealer` (combining dealer CRM + internal master data).
- Fact tables (`f_loan_contract`, `f_loan_balance_monthly`, `f_payment_transaction`,
  `f_default_event`, `f_dealer_performance_daily`) joining standardized keys.

This is where we apply cross-entity, multi-country logic:

- EU country codes.
- Multi-currency logic (e.g., base currency, FX conversions if required).
- Local regulatory flags (DORA, local data regulations).

---

## 6. Partitioning strategy

We consider two kinds of partitioning:

### 6.1. Physical / technical partitioning

For BigQuery, Redshift, Snowflake, etc.:

- Partition large fact tables by **date**:
  - `origination_date`, `transaction_date`, `calendar_month`.
- Optionally cluster by:
  - `customer_key`, `dealer_key`, `country_code`.

Benefits:

- Faster queries, lower cost.
- Naturally aligns with reporting periods.

### 6.2. Logical / organizational partitioning

Separate schemas/datasets by domain while keeping shared dimensions:

- `core` – core customer/product/dealer/date tables.
- `finance_mart` – financial fact tables.
- `risk_mart` – risk & default fact tables.
- `sales_mart` – sales & dealer performance.

Each mart reuses conformed dimensions, so cross-domain analysis is easy.

---

## 7. Connecting the warehouse to business tools

### 7.1. BI self-service & dashboards

- BI tools (Power BI, Qlik, Looker, etc.) connect directly to the **Gold layer**.
- Build semantic models / star schemas in BI:
  - Risk dashboards (exposure, defaults, arrears).
  - Finance dashboards (interest income, fees, provisions).
  - Dealer dashboards (applications, approvals, financed amounts, KPIs).

Internal portals can embed dashboards via:

- Native embedding capabilities (Power BI Embedded, Qlik Mashups).
- iFrames in internal web tools.

### 7.2. API layer on top of the DWH

Optionally, build a service layer (e.g. FastAPI, Node.js, .NET):

- Example endpoints:
  - `GET /customer/{id}/loan-history`  
    → joins `dim_customer`, `f_loan_contract`, `f_default_event`.
  - `GET /dealer/{id}/kpi?from=2024-01-01&to=2024-12-31`  
    → queries `f_dealer_performance_daily`.
  - `GET /portfolio/risk-summary?country=DE`  
    → aggregates `f_loan_balance_monthly` + `f_default_event`.

Frontend apps for:

- **Credit officers** (customer risk view, loan history).
- **Dealer managers** (dealer performance, returns).
- **Management** (portfolio and P&L overview).

All of them consume from the single source of truth: the **Gold layer** of the DWH.

---

## 8. Cloud-agnostic mapping (BigQuery → AWS / Azure)

Although this demo uses **BigQuery**, the design is cloud-agnostic.

| Concept               | GCP (demo)                         | AWS equivalent                   | Azure equivalent              |
|-----------------------|------------------------------------|----------------------------------|------------------------------|
| Object storage        | GCS                                | S3                               | ADLS                         |
| Raw zone              | `ryoji_raw_demos`                  | Glue tables over S3 (Bronze)     | Synapse / Delta on ADLS      |
| Warehouse / marts     | BigQuery datasets                  | Redshift / Athena                | Synapse / Fabric             |
| Transformations       | dbt on BigQuery                    | dbt + Redshift/Athena/Glue       | dbt + Synapse/Fabric         |
| Orchestration         | Airflow / Cloud Composer           | MWAA / Step Functions / Glue     | Data Factory / Synapse       |
| Infra as Code         | Terraform `google_*` resources     | Terraform `aws_*` resources      | Terraform `azurerm_*`        |

In an interview you can say:

> “I implemented this on BigQuery due to account constraints, but the warehouse
> design and ETL patterns (raw → staging → marts, dimensional modeling,
> domain-driven schemas) are identical to what we would run on AWS with S3, Glue,
> Athena/Redshift and dbt. The only changes are in the infrastructure provider
> and connection details.”

---

## 9. Repository structure (example)

An example repo layout using this README:

```text
etl-demos/
  README.md

  data/                     # source CSV/XLSX samples (e.g. auto loan dataset)
    auto_loan_default.csv
    customers.csv

  scripts/
    load_raw_to_bq.py       # load CSV/XLSX into raw BigQuery tables

  dbt/
    dbt_project.yml
    profiles.yml            # local dev profile (excluded in real projects)
    models/
      staging/
        stg_corebank_loans.sql
        stg_corebank_customers.sql
        staging.yml
      marts/
        core/
          dim_customer.sql
          dim_product.sql
          dim_dealer.sql
          dim_date.sql
          dim_contract_status.sql
          f_loan_contract.sql
          f_loan_balance_monthly.sql
          f_payment_transaction.sql
          f_default_event.sql
          f_dealer_performance_daily.sql
        marts_core.yml

  dags/
    bank_etl_dag.py   # Airflow DAG orchestrating ingestion + dbt

  terraform/
    main.tf
    variables.tf
    outputs.tf

  .github/
    workflows/
      ci_dbt.yml            # optional CI to run `dbt build` on PRs

  requirements.txt          # dbt, google-cloud-bigquery, airflow, etc.
