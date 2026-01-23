# Banking DWH → RDF/OWL → Neo4j (n10s) + SHACL + GraphRAG

This path adds a **semantic layer** (RDF/OWL ontology) on top of the warehouse, exports data as **Turtle (.ttl)**, validates it with **SHACL**, then imports both **ontology + data** into **Neo4j** using **Neosemantics (n10s)**.

---

## 1) Ontology (semantic model)

### What you already have
Your `kg/ontology/hb_bank.ttl` includes:
- an `owl:Ontology` header with label/comment
- core classes (`Customer`, `Dealer`, `Country`, `Vehicle`, `Contract`, `DefaultEvent`)
- object + datatype properties with `rdfs:domain` / `rdfs:range`

### What to add next (recommended)
To make the ontology usable for humans and downstream GenAI:
- `rdfs:label` and `rdfs:comment` for **each class/property**
- optional `skos:definition` for crisp business definitions
- (optional) `owl:FunctionalProperty` for key properties

---

## 2) Warehouse quality gates (Silver/Gold) with dbt

Run:

```bash
cd dbt
dbt run
dbt test
```

---

## 3) Export marts → Turtle (.ttl)
Use the exporter in `kg/export/` (e.g., `export_bank_kg_data_ttl.py`) to generate a data graph in Turtle.

Good practice:
- Mint stable IRIs for each entity (Customer/Contract/Dealer/Vehicle/…)
- Emit object-property triples for relationships
- Emit literal triples for measures/dates (datatype properties)

---

## 4) Validate RDF with SHACL (fail fast)
Your shapes live in: `kg/shacl/bank_shapes.ttl`.

Example validator:
```bash
pip install pyshacl rdflib
pyshacl -s kg/shacl/bank_shapes.ttl -d kg/neo4j/import/hb_bank_data.ttl
```

---

## 5) Load ontology + data into Neo4j (n10s)

### Start Neo4j
```bash
cd kg/neo4j
docker compose up -d
```

### Run the provided Cypher scripts
- `00_constraints.cypher`
- `01_n10s_init.cypher`
- `02_import_ontology_and_data.cypher`
- `03_validate.cypher`

---

## 6) Validate in Neo4j (integrity + reconciliation)
Optionally compare marts row counts to Neo4j counts using `kg/scripts/neo4j_counts.py`.

---

## 7) GraphRAG interface (indexing + retrieval)

### Recommended schema (minimal)
- `(:Document {doc_id, source, title})`
- `(:Chunk {chunk_id, text, embedding})`
- `(:Chunk)-[:MENTIONS]->(:Customer|:Contract|...)`

### Create indexes (example)
```cypher
// Vector index for embeddings (adjust dimension & similarity)
CREATE VECTOR INDEX chunk_embedding IF NOT EXISTS
FOR (c:Chunk)
ON (c.embedding)
OPTIONS {indexConfig: {`vector.dimensions`: 384, `vector.similarity_function`: 'cosine'}};

// Full-text index for keyword search
CREATE FULLTEXT INDEX chunk_text IF NOT EXISTS
FOR (c:Chunk)
ON EACH [c.text];
```

### Neo4j GraphRAG Python package (optional)
Neo4j provides a GraphRAG Python package with retrievers/pipelines for GenAI apps.

---

## Orchestration (Airflow)
A practical “fail-fast” ordering is:

`dbt_run → dbt_test → export_ttl → shacl_validate → neo4j_import → post_validate`
