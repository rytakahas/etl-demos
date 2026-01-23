# Banking DWH → Neo4j Knowledge Graph (Property Graph)

This repo demonstrates a **banking analytics pipeline** that produces a **dimensional warehouse (dbt)** and then projects it into a **Neo4j Knowledge Graph** (property graph) for graph analytics and (optionally) GenAI/GraphRAG use cases.

> For the **RDF/OWL semantic layer + Turtle + SHACL** path, use: **`README_BANK_KG_RDF.md`**.

---

## Data flow (Bronze → Silver → Gold → KG)

The pipeline follows a **medallion-style** progression:

- **Bronze**: raw source tables landed as-is (CSV/source tables)
- **Silver**: cleaned/staged models (conformed types, deduping, stable keys)
- **Gold**: analytics marts (dim/fact star schema)
- **KG projection**: Neo4j nodes/relationships optimized for graph queries (property graph)

### Quality gates

- **dbt tests** on Silver/Gold models (uniqueness, not-null, relationships, accepted values).
- **Neo4j constraints** to enforce entity-key integrity (MERGE safety).
- Optional **row-count reconciliation** from marts → Neo4j.

Airflow runs these as tasks inside a DAG, so you can stop the run when checks fail.

---

## Repository map

```
dbt/
  models/staging/           # Silver (stg_*) + tests in staging.yml
  models/marts/core/        # Gold dims/facts + marts_core.yml tests
dags/
  bank_etl_dag.py           # orchestrates raw → dbt → marts
  bank_kg_rdf_dag.py        # RDF/OWL semantic path (see README_BANK_KG_RDF.md)
kg/
  export/                   # export marts → KG artifacts (RDF/TTL exporters live here)
  neo4j/
    docker-compose.yml
    cypher/
      00_constraints.cypher
      03_validate.cypher
  scripts/
    neo4j_counts.py         # reconcile counts (optional)
include/
  load_raw_to_bq.py         # example raw ingestion helper (optional)
```

---

## Quickstart (local, Docker + dbt + Airflow)

### 0) Prereqs

- Docker / Docker Desktop
- Python
- dbt
- Airflow (Astro or vanilla)

### 1) Build Silver + Gold (dbt)

Run dbt models and tests:

```bash
cd dbt
dbt deps
dbt run
dbt test
```

Your staging tests are defined in `dbt/models/staging/staging.yml`.

### 2) Start Neo4j (for property graph usage)

Bring up Neo4j:

```bash
cd kg/neo4j
docker compose up -d
```

Open Neo4j Browser and run constraints:

```cypher
// kg/neo4j/cypher/00_constraints.cypher
```

### 3) Project marts → Neo4j property graph

Use exporters under `kg/export/` to create the artifacts you load into Neo4j (CSV or Cypher, depending on your script).

Typical banking shape:

- **Nodes**: Customer, Contract, Dealer, Vehicle, Country
- **Relationships**: `(:Contract)-[:HAS_CUSTOMER]->(:Customer)` etc.

### 4) Validate

Run `kg/neo4j/cypher/03_validate.cypher` and optionally compare counts:

```bash
python kg/scripts/neo4j_counts.py
```

---

## Knowledge Graph Design (KGD): practical thumb rules

### Mapping (DWH → property graph)

Default mapping:

- **Dimensions → Nodes** (entity tables become labels)
- **Facts → Relationships OR Event nodes**
  - if a fact is "binary" (connects two entities), use a relationship with properties
  - if a fact is "n-ary" (transaction touches many dims), model it as an event node

### Keys + integrity

Banking KGs live and die by stable keys:

- Put an immutable key on each entity node (e.g., `customer_id`, `contract_id`)
- Enforce uniqueness with Neo4j constraints so `MERGE` stays safe

### Performance

- Prefer "lookup by key → traverse"
- Add indexes/constraints on identifiers
- Keep relationship types meaningful (query readability + planner hints)

---

## Next step: GraphRAG (optional)

If you want a GraphRAG-ready layer (documents + chunks + embeddings + links to entities), follow the pattern in `README_BANK_KG_RDF.md` (GraphRAG section) and create:

- `(:Document)-[:HAS_CHUNK]->(:Chunk {text, embedding})`
- `(:Chunk)-[:MENTIONS]->(:Customer|:Contract|...)`

---

## Troubleshooting

- **If dbt tests fail**: inspect `dbt/logs/dbt.log` and fix upstream data or add cleaning logic.
- **If Neo4j MERGE creates duplicates**: verify uniqueness constraints and key mapping.
- **If ports are in use**: stop old containers or remap ports in `docker-compose.yml`.
