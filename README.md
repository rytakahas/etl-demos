# etl-demos — Bank DWH Design (Star Schema)

This repo is a **mini Data Warehouse (DWH) demo** inspired by **European bank** analytics use-cases in **auto / dealer financing**: **sales financing**, **portfolio & profitability**, **dealer performance**, and **credit risk**.

It is designed to be:
- **Cloud-agnostic** (maps cleanly to AWS / GCP / Azure)
- **Practical** (ERD + SQL query pack)
- **Bank-ready analytics model** (clear grain, keys, SCD, KPI mapping for auto/dealer finance)

---

## What's Inside (At a Glance)

### Design Artifacts
- **ERD (DBML):** [`bank.dbml`](./bank.dbml) *(open in dbdiagram.io / dbdiagram.com)*
- **Model notes:** [`bank.txt`](./bank.txt) *(table-by-table notes: facts vs dims, keys, history rules)*

### Query Pack
- **`scripts/sql/`** — ready SQL examples:
  - profit, EV vs ICE trends, dealer ranking, risk-adjusted profit

> Implementation can be done with dbt + a warehouse (Redshift/BigQuery/Snowflake/Postgres).  
> The focus of this repo is **modeling + analytics patterns** (the "why" and "how").

---

## 0) Modeling Framework Used (Conceptual → Logical → Physical)

Think of it like: **Business → Blueprint → Implementation**

| Layer | What it answers | What it looks like in this repo |
|-------|-----------------|----------------------------------|
| **Conceptual** (business view) | *What exists and why?* Processes, entities, relationships, KPIs | **Sections 1–2** (business questions, grain, core entities) |
| **Logical** (tech-agnostic design) | *How do we represent it?* Facts/dims, keys, relationships, history rules (SCD) | **`bank.dbml`** + Sections 3–6 |
| **Physical** (implementation details) | *How is it stored/optimized?* DDL, partitions, clustering/sort keys, indexes, constraints, tests | **Section 8** + `scripts/sql/` |

---

## 1) Business Goals (Conceptual Model)

### Typical Business Questions (KPIs)
This bank analytics demo targets questions like:

- **Portfolio growth**: contracts originated over time, by country/entity, dealer, product
- **Profitability**: margin and margin proxy by dealer/product/vehicle type
- **Dealer performance**: ranking, contribution, EV adoption trends
- **Credit risk**: delinquency / NPL, defaults, recoveries, risk-adjusted profit

### Core Entities (Conceptual)
- **Customer** signs **Contract** via **Dealer**
- Contract relates to **Product** (loan/lease/etc.) and **Vehicle** (ICE/Hybrid/EV)
- Contract produces **Payments**
- Contract has **Balances over time** (snapshots)
- Some contracts become **Default events** with **Recoveries**

> Domain context: customers finance vehicle purchases through dealers (loan/lease/PCP), generating payments, balances, and potential default/recovery events.

---

## 2) Grain Statements (Conceptual → Logical Bridge)

State **grain** early (what one row means).

This demo uses these grains:

- `f_contract_retail`: **1 row per retail contract** (origination event)
- `f_payment`: **1 row per payment / scheduled installment**
- `f_balance_daily`: **1 row per contract per date** (daily or month-end snapshot)
- `f_default_event`: **1 row per default event per contract** (default + recoveries)

Dimensions are descriptive lookup tables used to slice facts: customer, dealer, vehicle, product, date, country/entity, status…

---

## 3) Schemas to Know (When to Use Which)

### A) Star Schema (Kimball) — Recommended for Bank Analytics (Gold Layer)
**Shape:** a central **fact** table connected to multiple **dimension** tables.

- **Fact tables** hold *events/snapshots* (contracts, payments, balances, defaults)
- **Dimension tables** hold *descriptions* (customer, dealer, product, vehicle, date, country/entity)

Why it fits bank analytics:
- Most questions are **aggregations** (volume/profit/risk by time/country/dealer/product)
- BI tools (Tableau/Power BI) are optimized for facts + dims
- Easy to extend and easy to explain

> **Star schema is intentionally denormalized for analytics.**  
> Repeating foreign keys across fact rows is expected.

### B) Snowflake Schema — More Normalized Dimensions
A snowflake schema is a star schema where dimensions are split into sub-dimensions.

Example: `dim_customer` → `dim_city` → `dim_region` → `dim_country`

**Pros:**
- Less duplication inside dimensions
- Useful for strict governance / large hierarchies

**Cons:**
- More joins, more complexity for analysts
- Often not worth it for a first analytics milestone

### C) One Big Table (OBT) — Serving Layer (Not the "Truth")
**OBT** is a wide denormalized table or view that pre-joins facts + dims.

**Good for:**
- Dashboards and ad-hoc analysis
- "No-join" analyst experience

**Not ideal as the core truth layer:**
- Harder to enforce semantics and history
- Can hide grain mistakes

**Recommendation:** keep **Star** as truth, optionally provide **OBT views** on top.

### D) Where Data Vault Fits (Optional Silver Integration Layer)
Data Vault is often used between raw ingestion and marts when you need:
- Multi-source integration
- Auditable history (append-only changes)
- Clear lineage + traceability

**Common building blocks:**
- **Hubs**: business keys (Customer, Contract, Dealer)
- **Links**: relationships (Customer–Contract, Dealer–Contract)
- **Satellites**: attributes + history (Customer attributes over time)

**Typical modern layering:**
- **Bronze**: raw landing (files/events)
- **Silver**: integration (3NF or Data Vault H/L/S)
- **Gold**: star schema marts (+ optional OBT views)

> Data Vault doesn't replace the star; it usually **feeds** it.

---

## 4) Logical Model (Star Schema) — From `bank.dbml`

Open **[`bank.dbml`](./bank.dbml)** in dbdiagram.io to see the ERD.

### 4.1 Dimension Tables
- `dim_country_entity` — geography + legal entity / partner bank
- `dim_customer` — customer / borrower (**SCD2**) with `valid_from/valid_to/is_current`
- `dim_dealer` — dealer / channel partner (often SCD1; can be SCD2 if required)
- `dim_vehicle` — vehicle model (fuel type: ICE / Hybrid / EV)
- `dim_product` — finance product (PCP/Loan/Leasing/Renting/Insurance flags)
- `dim_contract_status` — standardized statuses + "default status" flag
- `dim_date` — calendar table (1 row per date)

### 4.2 Fact Tables
- `f_contract_retail` — retail finance origination
- `f_contract_wholesale` — dealer floorplan / wholesale lines (optional extension)
- `f_payment` — payments / scheduled installments
- `f_balance_daily` — daily or month-end contract exposure snapshots
- `f_default_event` — defaults + recoveries events

### Star Mental Picture

```
        dim_customer     dim_dealer
              \             /
               \           /
                \         /
           f_contract_retail (FACT)
                /    |    \
               /     |     \
      dim_vehicle  dim_product  dim_date
                     |
              dim_country_entity
```

---

## 5) Keys, Surrogate Keys, and SCD (Slowly Changing Dimensions)

### 5.1 Surrogate Keys (SK)
A **surrogate key** is a warehouse-generated integer key (e.g. `customer_key`) used as the PK in dimensions.

Why it matters in a bank setting:
- Multiple source systems → ID collisions and changing IDs
- SKs are stable and efficient for joins

### 5.2 SCD2 (History-Preserving Dimensions)
**SCD2** keeps full history by inserting a new dim row when attributes change.

**Typical SCD2 candidates:**
- `dim_customer` (segment, risk band, address)
- `dim_product` (product definitions, terms)
- `dim_dealer` (if partner status/region history matters)

**Common SCD2 fields:**
- `valid_from`, `valid_to`, `is_current`

Facts should join to the correct dim version for "as-of" reporting.

---

## 6) Risk Metrics (PD / NPL / LGD / EAD) — Conceptual Mapping

**Key definitions:**
- **PD**: Probability of Default over a horizon (e.g., 12 months)
- **NPL**: Non-Performing Loan (often 90+ DPD or unlikely to pay)
- **LGD**: Loss Given Default (fraction lost if default happens)
- **EAD**: Exposure at Default (amount outstanding at default)

**Expected Loss mental model:**

```
Expected Loss ≈ PD × LGD × EAD
```

In this schema:
- **PD** signals/features come from payments + balances + contract attributes
- **NPL** status comes from DPD + contract_status (often in balances/snapshots)
- **LGD** is estimated from defaults & recoveries in `f_default_event`
- **EAD** is proxied from outstanding balance in `f_balance_daily`

---

## 7) Repo Layout (Implementation-Ready)

A typical layered implementation looks like:

```
.
├── data/                # sample raw extracts (optional)
├── bank.dbml            # ERD (logical model)
├── bank.txt             # model notes (facts/dims, keys, history rules)
└── scripts/
    └── sql/             # analytics queries
```

If you extend to a full ETL/dbt demo, a common structure is:

```
├── dags/                # Airflow orchestration (optional)
├── dbt_project/         # staging → marts (dims/facts)
└── models/ or ddl/      # warehouse DDL, constraints, docs
```

---

## 8) Physical Modeling Notes (Performance, Skew, and Tests)

### 8.1 Physical Tuning Principles
Large facts (`f_payment`, `f_balance_daily`) should be optimized around query patterns:
- Filtering by date ranges
- Joining by `contract_key`, `customer_key`, `dealer_key`
- Aggregating by time/country/fuel type

#### Redshift-Style Guidance (Example)
- `SORTKEY` by date on snapshot facts (time range queries)
- `DISTKEY` on high-cardinality join keys like `contract_key` to reduce shuffle
- Avoid distkey on low-cardinality fields (e.g., `country_entity_key`) to reduce skew

#### BigQuery-Style Guidance (Example)
- Partition large facts by `date_key` / `event_date`
- Cluster by frequently filtered/joined keys (`contract_key`, `dealer_key`)
- Use incremental loads and pre-aggregations for heavy dashboards

### 8.2 Data Quality Tests
Add automated checks (dbt tests or SQL assertions):
- **Uniqueness**: contract business key uniqueness in the correct grain
- **Not nulls**: keys and critical measures
- **Referential integrity**: fact FK keys exist in dimensions
- **Reconciliation**: sums/counts between staging and marts for a load window

---

## 9) SQL Query Pack (scripts/sql/)

Included queries cover:
- Volume by segment
- Dealer profit (best case: `est_lifetime_margin_eur`)
- Dealer profit proxy (interest rate − funding rate)
- Risk-adjusted profit (margin − expected loss / defaults)
- Dealer ranking by (country, fuel_type) using window functions
- Monthly EV/Hybrid/ICE share trend

Example filenames:
- `query_customer_segment.sql`
- `query_dealer_profit_margin_best.sql`
- `query_dealer_profit_margin_proxy.sql`
- `query_dealer_profit_margin_minus_defaults.sql`
- `query_dealer_rank_by_country_fuel.sql`
- `query_monthly_fuel_trend.sql`

---

## 10) Extension: From Star Schema → Knowledge Graph (GraphRAG-ready)

A star schema is the best default for aggregations and KPI reporting. A knowledge graph becomes valuable when your questions are relationship-heavy, require multi-hop reasoning, or need explainable paths (e.g., networks of dealers/customers/contracts, fraud rings, entity resolution across sources, lineage/explanations for "why did this risk score change?").

### When a Graph Helps (Typical Bank Use-Cases)
- **Fraud / collusion patterns**: shared phone/address, repeated VIN/vehicle across customers, suspicious dealer clusters
- **Entity resolution**: merge "same" customers/dealers across systems (aliases, slightly different identifiers)
- **Dealer network analysis**: communities, centrality, propagation effects (e.g., defaults concentrated in connected partners)
- **Explainable risk paths**: "contract → dealer → region → historical default cluster"
- **GraphRAG**: retrieve structured neighborhoods (k-hop subgraphs) as grounded context for an LLM

### How It Fits the Layering
A common hybrid architecture is:
- **DWH (Star)** = truth for metrics (profitability, portfolio KPIs, risk KPIs)
- **Graph** = truth for relationships (who/what connects to what, and how)

You can export curated "gold" entities/relationships from the star schema into a graph store:
- **Nodes**: Customer, Dealer, Contract, Vehicle, Product, Country/Entity
- **Edges**: `CUSTOMER_SIGNED_CONTRACT`, `CONTRACT_AT_DEALER`, `CONTRACT_HAS_VEHICLE`, `CONTRACT_HAS_PRODUCT`, `CUSTOMER_LIVES_IN_REGION`, etc.

### Guides (Linked)
This repo is warehouse-first (star schema for KPI analytics). If you want to expand it with a data lake ingestion layer and/or a knowledge graph layer, these docs cover each path:

**Data lake / raw ingestion (CSV loading patterns):**
- `LOADING_NEW_DATA` (General "how to load CSV/new data" guide — not KG-specific.)

**Knowledge Graph (Property Graph / Neo4j-style):**
- `README_BANK_KG`

**Knowledge Graph (RDF / ontology-first):**
- `README_BANK_KG_RDF`

Practical rule: start with the star schema (fast KPI value), then add (1) a lake ingestion layer for scalable raw inputs, and/or (2) a graph layer for relationship-first analytics and GraphRAG use cases.

---

## References

- Kimball dimensional modeling (star schema)
- Basel / IFRS-9 concepts (PD/LGD/EAD terminology)

---

## Notes

All names and structures are demo-oriented and do not represent any real bank production schema.
