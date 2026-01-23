# Banking DWH → RDF/OWL → Neo4j (n10s) + SHACL + GraphRAG

This path adds a **semantic layer** (RDF/OWL ontology) on top of the warehouse, exports data as **Turtle (.ttl)**, validates it with **TTL lint + SHACL**, then imports both **ontology + validated data** into **Neo4j** using **Neosemantics (n10s)**.

This is the "serious" KG pipeline:
- Canonical ontology (TBox) is frozen in version control
- Data exports are validated before they touch Neo4j
- Neo4j imports the **validated enriched** TTL (not raw)

---

## 1) Canonical KG schema (freeze the contract)

### TBox (ontology)
- `kg/ontology/hb_bank.ttl`
  - `owl:Ontology` header (label/comment/version)
  - core classes (`Customer`, `Dealer`, `Country`, `Vehicle`, `Contract`, `Payment`, `DefaultEvent`, …)
  - object + datatype properties with `rdfs:domain` / `rdfs:range`
  - canonical predicates used downstream (`amountPaid`, `daysPastDue`, `eventType`, `model`, …)

### Constraints (SHACL)
- `kg/shacl/hb_bank.shapes.ttl` (canonical shapes)
  - defines "required" vs "optional" fields (fail fast)

### Validation gates (must pass)
- `kg/validation/validate_ttl.py`  
  TTL parse + lint (schema drift, undeclared predicates, missing labels/domain/range)
- `kg/validation/validate_shacl.py`  
  SHACL validation (cardinality, types, required fields)

---

## 2) Warehouse quality gates (Silver/Gold) with dbt

Run:

```bash
cd dbt
dbt run
dbt test
```

---

## 3) Export marts → Turtle (.ttl) (raw)

Exporter:

```
kg/export/export_bank_kg_data_ttl.py
```

Guidelines:

- mint stable IRIs (Customer/<id>, Contract/<id>, …)
- emit object-property triples for relationships
- emit typed literals for measures/dates

Example:

```bash
python kg/export/export_bank_kg_data_ttl.py \
  --data-dir data \
  --out kg/ontology/hb_bank_data.ttl
```

---

## 4) Enrichment (config-driven safety net) → Turtle (enriched)

Enrichment exists to reduce fragility and minimize code edits when datasets vary.
Rules are declarative:

```
kg/config/enrichment_rules.yaml
```

- default unknown nodes (e.g., Customer/UNKNOWN)
- alias predicates (paidAmount → amountPaid)
- required field synthesis (demo-friendly defaults)

Enricher:

```
kg/export/enrich_bank_data_ttl.py
```

Example:

```bash
python kg/export/enrich_bank_data_ttl.py \
  --config kg/config/enrichment_rules.yaml \
  --in  kg/ontology/hb_bank_data.ttl \
  --out kg/ontology/hb_bank_enriched.ttl
```

---

## 5) Validate RDF (fail fast)

### 5.1 TTL lint (schema drift + hygiene)

```bash
python kg/validation/validate_ttl.py \
  --in kg/ontology/hb_bank.ttl kg/ontology/hb_bank_enriched.ttl \
  --out kg/export/ttl_report.json
```

### 5.2 SHACL (data integrity)

```bash
python kg/validation/validate_shacl.py \
  --data kg/ontology/hb_bank_enriched.ttl \
  --shapes kg/shacl/hb_bank.shapes.ttl \
  --ontology kg/ontology/hb_bank.ttl \
  --inference rdfs \
  --out kg/export/shacl_report.ttl
```

Only validated TTL should be loaded into Neo4j.

---

## 6) Load ontology + validated data into Neo4j (n10s)

### Start Neo4j (Docker on Mac)

```bash
cd kg/neo4j
docker compose up -d
```

**Important**: Neo4j imports from `/var/lib/neo4j/import`.
This repo mounts `kg/ontology/` into Neo4j import dir, so Neo4j can load:

- `hb_bank.ttl`
- `hb_bank_enriched.ttl`

### Run the provided Cypher scripts

- `kg/neo4j/cypher/00_constraints.cypher`
- `kg/neo4j/cypher/01_n10s_init.cypher`
- `kg/neo4j/cypher/02_import_ontology_and_data.cypher`
  (should import hb_bank.ttl + hb_bank_enriched.ttl)
- `kg/neo4j/cypher/03_validate.cypher`
- `kg/neo4j/cypher/04_graphrag_indexes.cypher` (GraphRAG-ready indexes)

---

## 7) Validate in Neo4j (integrity + reconciliation)

Graph DQ queries:

- `kg/neo4j/cypher/03_validate.cypher`
- `kg/neo4j/cypher/queries/*.cypher`

Optional reconciliation:

```
kg/scripts/neo4j_counts.py
```

---

## 8) GraphRAG interface (indexes + retrieval)

### Neo4j-side indexes (already scripted)

```
kg/neo4j/cypher/04_graphrag_indexes.cypher
```

### Optional doc indexing (unstructured → Neo4j)

Script:

```
kg/graphrag/index_graphrag_docs.py
```

Target schema (minimal):

- `(:Document {doc_id, source, title})`
- `(:Chunk {chunk_id, text, embedding})`
- `(:Chunk)-[:MENTIONS]->(:Customer|:Contract|...)`

---

## Orchestration (Airflow)

A practical "fail-fast" ordering:

```
dbt_build → export_ttl → enrich_ttl → validate_ttl → validate_shacl → neo4j_import → post_validate
```

Notes:

- avoid multiple DAGs writing the same `hb_bank_enriched.ttl` concurrently
- publish "latest" artifacts only after validation passes

---

## Troubleshooting

### Port conflicts (Neo4j/Postgres)

If you see "port already allocated", stop old containers or remap ports in docker-compose.

### Neo4j connectivity from Airflow

Avoid connecting to the wrong Neo4j instance:

- keep one "publisher" pipeline writing `hb_bank_enriched.ttl`
- ensure Airflow connects to the correct host/port and credentials

### SHACL fails

Inspect `kg/export/shacl_report.ttl` to see which nodes violated which rule, then:

- adjust enrichment rules (`kg/config/enrichment_rules.yaml`)
- or adjust shapes if a field should be optional (rare; prefer enrichment)
