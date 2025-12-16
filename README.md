# etl-demos — Bank DWH design (Star schema) + interview SQL

This repo is a **mini, interview-ready Data Warehouse (DWH) demo** inspired by **A Bank Europe** analytics use‑cases:
**sales financing**, **portfolio / profitability**, **dealer performance**, and **credit risk**.

It has **two layers of “design artifacts”**:
- **`bank.dbml`** — the **Star Schema ERD** (paste into dbdiagram.io / databasediagram.com)
- **`bank.txt`** — a **mock interview script** (table‑by‑table explanation: facts vs dimensions, keys, scaling)

…and a practical **query pack**:
- **`scripts/sql/`** — ready SQL examples (profit, EV vs ICE trends, dealer ranking, risk‑adjusted profit)

> This demo is implemented in a cloud‑agnostic way, but the design maps cleanly to
> **AWS lakehouse → Redshift DWH** (JD), and can also run on **BigQuery** for a portfolio/demo.

---

## 1) Schemas to know for interviews (and when to use them)

### A) Star schema (Kimball) — recommended for a Bank analytics
**Shape:** a big **fact table** in the center, connected to multiple **dimension tables**.

- **Fact tables** hold *events or snapshots* (contracts, payments, balances, defaults).
- **Dimension tables** hold *descriptions* (customer, dealer, product, vehicle, date, country/entity).

Why it fits Bank:
- Most questions are **aggregation questions** (volume/profit/risk by time, country, dealer, product, fuel type).
- BI tools (Power BI/Qlik/Tableau) are optimized for **facts + dimensions**.
- It’s easy to explain and easy for analysts to query.

**Important point:** Star schema is intentionally **denormalized for analytics**.
That means:
- The *same dimension keys* appear in many fact rows (repetition is expected).
- Dimension attributes are kept in *one* place (the dimension), but facts “repeat” the FK values across rows.

### B) Snowflake schema — “more normalized dimensions”
A snowflake schema is like a star schema, **but dimensions are split into sub‑dimensions**.

Example:
- `dim_customer` → `dim_city` → `dim_region` → `dim_country`

Pros:
- Less duplication inside dimension attributes.
- Can improve governance if you have very shared attributes (geo hierarchies).

Cons:
- More joins → more complexity for analysts and dashboards.
- Often not worth it unless dimensions are huge/complex or strictly governed.

### C) 3NF / Inmon EDW (normalized enterprise warehouse)
This is a **fully normalized** enterprise model (many tables, many joins).
Often used as:
- a “core” integration layer (system-of-record style),
- feeding an analytics star schema downstream.

Pros:
- Strong data integrity, good for operational consistency and integration.
Cons:
- Harder for analytics users; not what you want as the *first* analytics milestone.

**Interview phrasing you can use:**
> “For the first milestone (analytics DWH), I’d deliver a Kimball star schema in the Gold layer.
> If they also need an enterprise integration core, we can keep a more normalized model in Silver.”

---

## 2) What is a FACT table “at the center” (and why not customer‑centric)?

Bank’s business is *customer‑centric*, but the **data model for analytics** is **process‑centric**.

The center is a **business process** with measurable outcomes:
- **Contract origination** → `f_contract_retail`
- **Payments** → `f_payment`
- **Portfolio exposure (daily/monthly)** → `f_balance_daily`
- **Defaults & recoveries** → `f_default_event`
- **Dealer wholesale lines** → `f_contract_wholesale`

Why facts are central:
- Most questions start with “**How much / how many / how risky / how profitable**…”
- Those measures live in the facts, then we slice them by dimensions (dealer/country/product/vehicle/time).

A mental picture:

```
          dim_customer     dim_dealer
               \           /
                \         /
                 \       /
              f_contract_retail   (FACT)
               /   |       \
              /    |        \
      dim_vehicle  dim_product  dim_date
                    |
             dim_country_entity
```

---

## 3) Dimensions, surrogate keys, and SCD (Slowly Changing Dimensions)

### 3.1 Surrogate keys (SK)
A **surrogate key** is a warehouse‑generated integer key (e.g., `customer_key`) used as the **PK** in dimensions.

Why Bank needs SKs:
- Multiple source systems across countries (DE/ES/partner banks) → different ID formats, collisions.
- IDs can change in source systems; SKs stay stable in the warehouse.
- They make joins fast in analytics warehouses.

### 3.2 SCD (Slowly Changing Dimensions)
**SCD2** keeps full history by creating a new dimension row when attributes change.

Typical SCD2 candidates in Bank:
- `dim_customer` (segment, risk band, address changes)
- `dim_dealer` (region, partner status) — often SCD1, but SCD2 if history matters
- `dim_product` (product terms/labels) — usually SCD2 if you must report historically “as it was sold”

SCD2 pattern:
- `valid_from`, `valid_to`, `is_current`
- Facts join to the *correct* historical dimension version.

---

## 4) A star schema (from `bank.dbml`)

Open **`bank.dbml`** in dbdiagram.io to see the ERD.

### 4.1 Dimension tables
- `dim_country_entity` — geography + legal entity / partner bank
- `dim_customer` — customer / borrower (**SCD2**) with `valid_from/valid_to/is_current`
- `dim_dealer` — dealer / channel partner (SCD1 by default; can be SCD2 if required)
- `dim_vehicle` — vehicle model (fuel type: ICE / Hybrid / EV)
- `dim_product` — finance product (PCP/Loan/Leasing/Renting/Insurance flags)
- `dim_contract_status` — standardized statuses + “default status” flag
- `dim_date` — calendar table (one row per date)

### 4.2 Fact tables
- `f_contract_retail` — **one row per retail finance contract** (origination)
- `f_contract_wholesale` — dealer floorplan / wholesale credit lines
- `f_payment` — **one row per payment or scheduled instalment**
- `f_balance_daily` — **one row per contract per date** (daily or month‑end snapshots)
- `f_default_event` — one row per default event per contract (default + recoveries, LGD proxy)

---

## 5) Risk metrics: PD, NPL, LGD (and how they connect)

- **PD (Probability of Default)**: likelihood a contract defaults over a horizon (e.g., 12 months).
- **NPL (Non‑Performing Loan)**: loans that are seriously delinquent (often 90+ DPD) or unlikely to pay.
- **LGD (Loss Given Default)**: % loss if default happens.
- **EAD (Exposure at Default)**: amount outstanding at default (proxied from balances).

Expected Loss mental model:
`Expected Loss ≈ PD × LGD × EAD`

In this star schema:
- PD features come from **payments + balances + contract attributes**
- NPL is measured from **DPD + contract_status** in balances
- LGD is measured from **defaults & recoveries** in `f_default_event`

---

## 6) Repo layout (implementation / “bank load”)

This repo’s “implementation story” is a standard layered warehouse:

### 6.1 Directory structure (typical)
```
.
├── data/                      # raw CSVs / sample extracts
├── config/
│   └── raw_sources.yml        # metadata-driven ingest config (Airflow-friendly)
├── dags/
│   └── bank_etl_dag.py        # orchestration (load → dbt run → tests)
├── dbt/ or dbt_project/       # dbt models: staging → marts (dims/facts)
├── scripts/
│   ├── diagram_star_schema.txt
│   └── sql/                   # analytics queries you can demo in interview
├── bank.dbml                  # ERD
└── bank.txt                   # mock interview script
```

### 6.2 Layering (Bronze/Silver/Gold)
- **Bronze (raw)**: land files/extracts “as-is”
- **Silver (staging)**: cleaned/typed tables
- **Gold (marts / DWH)**: star schema facts + dims

> In AWS terms: Bronze/Silver in **S3**, Gold in **Redshift** (built by **dbt**).

---

## 7) SQL scripts (all included)

All “interview-ready” queries are in **`scripts/sql/`**:

- `query_customer_segment.sql`
- `query_dealer_profit_margin_best.sql`
- `query_dealer_profit_margin_proxy.sql`
- `query_dealer_profit_margin_minus_defaults.sql`
- `query_dealer_rank_by_country_fuel.sql`
- `query_monthly_fuel_trend.sql`

These cover:
- Volume by segment
- Dealer profit (best case: `est_lifetime_margin_eur`)
- Dealer profit (proxy margin from interest_rate − funding_rate)
- Risk‑adjusted profit (margin − credit losses)
- Dealer ranking by (country, fuel_type) using window functions
- Monthly EV/Hybrid/ICE share trend

---

## 8) Scaling & skew (what to say)

### Logical scaling (model stability)
Star schema scales naturally when adding:
- more countries (`dim_country_entity` grows),
- more contracts/payments (facts grow),
without redesign.

### Physical scaling (warehouse tuning)
- Partition/sort by **date** on large snapshot facts (`f_balance_daily`, `f_payment`)
- Distribute by **high-cardinality keys** (e.g., `contract_key`) to avoid skew
- Avoid distributing by low-cardinality keys (e.g., `country_entity_key`) → skew
- Add aggregate/mart tables or materialized views for dashboard workloads

---

## 9) References you can cite verbally
- Ralph Kimball & Margy Ross — *The Data Warehouse Toolkit* (star schema / dimensional modeling)
- Bill Inmon — *Building the Data Warehouse* (3NF EDW)
- Basel / IFRS 9 concepts (PD/LGD/EAD terminology)
