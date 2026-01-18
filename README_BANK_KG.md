# Banking DWH → Neo4j Knowledge Graph (Property Graph)

This repo demonstrates a **banking analytics pipeline** that produces a **dimensional warehouse (dbt)** and then projects it into a **Neo4j Knowledge Graph** (property graph) for graph analytics and (optionally) GenAI/GraphRAG use cases.

> For the **RDF/OWL semantic layer + Turtle + SHACL** path, use: **`README_BANK_KG_RDF.md`**.

---

### Data flow (Bronze → Silver → Gold → KG)
The pipeline follows a **medallion-style** progression:
- **Bronze**: raw source tables landed as-is
- **Silver**: cleaned/staged models (conformed types, deduping, stable keys)
- **Gold**: analytics marts (dim/fact star schema)
- **KG projection**: Neo4j nodes/relationships optimized for graph queries

### Quality gates
- **dbt tests** on Silver/Gold models (uniqueness, null checks, relationships, accepted values).
- **Neo4j constraints** to enforce entity-key integrity (`MERGE` safety).
- Optional **row-count reconciliation** from marts → Neo4j.

Airflow runs these as tasks inside a DAG, so you can stop the run when checks fail.

---

## Repository map

```
dbt/
  models/staging/     # Silver (stg_*) + tests in staging.yml
  models/marts/core/  # Gold dims/facts + marts_core.yml tests

dags/
  bank_etl_dag.py     # orchestrates raw → dbt → marts
  bank_kg_rdf_dag.py  # RDF path (see README_BANK_KG_RDF.md)

kg/
  export/             # export marts → KG artifacts
  neo4j/
    docker-compose.yml
    cypher/
      00_constraints.cypher
      03_validate.cypher
  scripts/
    neo4j_counts.py   # reconcile counts (optional)

include/
  load_raw_to_bq.py   # example raw ingestion helper
```

---

## Quickstart (local, Docker + dbt + Airflow)

### 0) Prereqs
- Docker
- Python
- dbt (run locally or in container)
- Airflow (Astro or vanilla)

### 1) Load raw data (Bronze)
Your raw CSVs live in `data/`. In this demo, raw data is loaded to a “raw” dataset (Bronze).
Example helper: `include/load_raw_to_bq.py` (adapt as needed).

### 2) Build Silver + Gold (dbt)
Run dbt models and tests:

```bash
cd dbt
dbt deps
dbt run
dbt test
```

**Your staging tests** are already defined in `dbt/models/staging/staging.yml` (e.g., `loan_id` unique/not_null, etc.).

### 3) Start Neo4j
Bring up Neo4j via:

```bash
cd kg/neo4j
docker compose up -d
```

Open Neo4j Browser and run constraints:

```cypher
// kg/neo4j/cypher/00_constraints.cypher
```

### 4) Export marts → Neo4j property graph
Use the exporter(s) in `kg/export/` to create the artifacts you load into Neo4j (CSV or Cypher, depending on your script).

Typical banking shape:
- Nodes: `Customer`, `Contract`, `Dealer`, `Vehicle`, `Country`
- Relationships: `(:Contract)-[:HAS_CUSTOMER]->(:Customer)` etc.

### 5) Validate
Run `kg/neo4j/cypher/03_validate.cypher` and optionally compare counts:

```bash
python kg/scripts/neo4j_counts.py
```

---

## Knowledge Graph Design (KGD): practical thumb rules

### Mapping (DWH → property graph)
**Default mapping**:
- **Dimensions → Nodes** (entity tables become labels)
- **Facts → Relationships OR Event nodes**
  - if a fact is “binary” (connects two entities), use a relationship with properties
  - if a fact is “n-ary” (transaction touches many dims), model it as an event node

### Keys + integrity
Banking KGs live and die by stable keys:
- Put an immutable key on each entity node (e.g., `customer_id`, `contract_id`)
- Enforce uniqueness with Neo4j constraints so `MERGE` stays safe

### Performance (optimization)
- Prefer “lookup by key → traverse”
- Add indexes/constraints on the identifiers you start from
- Keep relationship types meaningful (query readability + planner hints)

---

## Data quality in practice

### Bronze (raw)
- Schema/format sanity (columns exist, parseable dates)
- Duplicate raw IDs
- Basic drift checks (row counts vs yesterday)

### Silver (staging)
- `not_null` on IDs and required business attributes
- `unique` on business keys
- `relationships` (FK integrity) when you have dim/fact relationships

### Gold (marts)
- Business-rule tests (e.g., no negative balances; default_date >= origination_date)
- Reconciliation tests (sum of payments equals mart rollups)

### In Airflow
Wire tasks like:

`ingest_raw >> dbt_run >> dbt_test >> export_kg >> neo4j_load >> validate`

---

## Next step: GraphRAG (optional)
If you want a GraphRAG-ready layer (documents + chunks + embeddings + links to entities), follow the pattern in **`README_BANK_KG_RDF.md`** (GraphRAG section) and create:
- `(:Document)-[:HAS_CHUNK]->(:Chunk {text, embedding})`
- `(:Chunk)-[:MENTIONS]->(:Customer|:Contract|...)`

---

## Troubleshooting
- If dbt tests fail: inspect `dbt/logs/dbt.log` and fix upstream data or add cleaning logic.
- If Neo4j MERGE creates duplicates: verify uniqueness constraints and key mapping.
