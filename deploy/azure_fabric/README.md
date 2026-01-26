# Azure / Microsoft Fabric deployment runbook (Azure-first)

This folder contains **deployment artifacts** to run the end-to-end pipeline in a **client tenant**:

**ADLS/OneLake (Bronze) → Fabric Lakehouse/Warehouse (Silver/Gold) → KGD tables → RDF export (TTL) → TTL+SHACL gates → Neo4j (n10s import) → GraphRAG indexes**

> This repo avoids hardcoding tenant IDs / workspace IDs. Put environment-specific values in Key Vault or CI/CD secrets.

## 0) What is the contract (versioned API)
- Ontology (TBox): `kg/ontology/hb_bank.ttl`
- SHACL constraints: `kg/shacl/hb_bank.shapes.ttl`
- Quality gates: `kg/validation/validate_ttl.py`, `kg/validation/validate_shacl.py`
- Neo4j migrations/import: `kg/neo4j/cypher/00..04_*.cypher`
- Export + enrich: `kg/export/export_bank_kg_data_ttl.py`, `kg/export/enrich_bank_data_ttl.py`, `kg/config/enrichment_rules.yaml`

These files should be reviewed like an API.

## 1) Fabric workspace (GitOps)
1. Create Fabric workspaces: **dev / test / prod** (or just dev first).
2. Enable Fabric Git integration and connect the **dev workspace** to this repo/branch.
3. Import/author the Fabric items under `deploy/azure_fabric/fabric/`:
   - pipelines (Data Factory)
   - notebooks (Spark)
   - SQL scripts (Warehouse)

> Execution is usually via **schedule/event triggers** in Fabric. Use REST APIs mainly for inventory/monitoring.

## 2) Data landing (Bronze)
- Raw files arrive in ADLS Gen2 (or OneLake) paths like:
  - `.../bank/raw/payments/dt=YYYY-MM-DD/*.parquet`

Trigger options:
- **Event trigger** (file arrival) OR
- **Schedule trigger** (hourly/daily)

## 3) Silver/Gold in Fabric
- Bronze → Silver: notebooks/pipelines parse, dedupe, conform.
- Silver → Gold: Warehouse SQL (or dbt-in-container) builds marts.

See:
- `deploy/azure_fabric/sql/gold_marts/`
- `deploy/azure_fabric/sql/kgd/`

## 4) KGD tables (graph export layer)
Create graph-ready tables/views from Gold (idempotent, stable IDs):

- Nodes: `kg_node_customer`, `kg_node_contract`, `kg_node_payment`, `kg_node_default_event`, ...
- Edges: `kg_edge_contract_has_customer`, `kg_edge_contract_has_payment`, ...

## 5) RDF export + quality gates (Container Apps Job)
Recommended: run Python export/enrich/validate + Neo4j load as an **Azure Container Apps Job**.

Provided under:
- `deploy/azure_fabric/jobs/kg_export_validate_load/`

## 6) Neo4j load + post-load checks
The job runs:
- `kg/neo4j/cypher/00_constraints.cypher`
- `kg/neo4j/cypher/01_n10s_init.cypher`
- `kg/neo4j/cypher/02_import_ontology_and_data.cypher`
- `kg/neo4j/cypher/03_validate.cypher`
- `kg/neo4j/cypher/04_graphrag_indexes.cypher`

Import is expected to load the **validated enriched TTL** artifact.

## 7) GraphRAG indexing job (optional)
- `deploy/azure_fabric/jobs/graphrag_index/`

Runs on schedule to index documents to Neo4j.

## 8) Secrets + identity (Key Vault)
Use Terraform placeholders under `infra/azure/terraform/modules/`:
- Key Vault
- Managed identity + role assignments

At minimum store:
- `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`
- optional: `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_KEY`
