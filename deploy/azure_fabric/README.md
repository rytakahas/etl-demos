# Azure / Microsoft Fabric deployment runbook (Azure-first)

This folder contains **deployment artifacts** to run the end-to-end pipeline in a **client tenant**:

**ADLS/OneLake (Bronze) → Fabric Lakehouse/Warehouse (Silver/Gold) → KGD tables → RDF export (TTL) → TTL+SHACL gates → Neo4j (n10s import) → GraphRAG indexes**

> This repo avoids hardcoding tenant IDs / workspace IDs. Put environment-specific values in Key Vault or CI/CD secrets.

---

## 0) What is the contract (versioned API)

- Ontology (TBox): `kg/ontology/hb_bank.ttl`
- SHACL constraints: `kg/shacl/hb_bank.shapes.ttl`
- Quality gates: `kg/validation/validate_ttl.py`, `kg/validation/validate_shacl.py`
- Neo4j migrations/import: `kg/neo4j/cypher/00..04_*.cypher`
- Export + enrich: `kg/export/export_bank_kg_data_ttl.py`, `kg/export/enrich_bank_data_ttl.py`, `kg/config/enrichment_rules.yaml`

These files should be reviewed like an API.

---

## 1) Fabric workspace (GitOps)

1. Create Fabric workspaces: **dev / test / prod** (or just dev first).
2. Enable Fabric Git integration and connect the **dev workspace** to this repo/branch.
3. Import/author the Fabric items under `deploy/azure_fabric/fabric/`:
   - pipelines (Data Factory)
   - notebooks (Spark)
   - SQL scripts (Warehouse)

> Execution is usually via **schedule/event triggers** in Fabric. Use REST APIs mainly for inventory/monitoring.

---

## 2) What you run (end-to-end)

There are **two orchestration layers**:

### A) Fabric pipeline (Bronze → Silver → Gold → KGD)

Runs inside Fabric via **schedule/event trigger** (no manual clicking after setup).

### B) Container Apps Job (RDF export → validate → Neo4j load)

Runs your repo's Python + Neo4j cypher steps. It is triggered:
- either by **Fabric pipeline Web Activity** (recommended),
- or by a **separate schedule** (acceptable if the KGD refresh is predictable).

---

## 3) Data landing (Bronze)

Raw files arrive in ADLS Gen2 (or OneLake) paths like:
- `.../bank/raw/payments/dt=YYYY-MM-DD/*.parquet`

Trigger options (choose one):
- **Event trigger** (file arrival) OR
- **Schedule trigger** (hourly/daily)

---

## 4) Silver/Gold in Fabric

- Bronze → Silver: notebooks/pipelines parse, dedupe, conform.
- Silver → Gold: Warehouse SQL (or dbt-in-container) builds marts.

See:
- `deploy/azure_fabric/sql/gold_marts/`
- `deploy/azure_fabric/sql/kgd/`

---

## 5) KGD tables (graph export layer)

Create graph-ready tables/views from Gold (idempotent, stable IDs):

- Nodes: `kg_node_customer`, `kg_node_contract`, `kg_node_payment`, `kg_node_default_event`, ...
- Edges: `kg_edge_contract_has_customer`, `kg_edge_contract_has_payment`, ...

**Goal:** produce stable node IDs and edge endpoints so the export is repeatable.

---

## 6) How to run the pipeline (step-by-step)

### Step 1 — Build and publish the KG runner image (one-time, CI/CD)

Your runner image executes:
- export TTL
- enrich TTL
- validate TTL + SHACL
- load into Neo4j (n10s + constraints/indexes)

Files:
- `deploy/azure_fabric/jobs/kg_export_validate_load/Dockerfile`
- `deploy/azure_fabric/jobs/kg_export_validate_load/run.sh`

Example (CI/CD): build and push to ACR

```bash
# Example only: adapt to your ACR and CI
ACR_NAME=<your-acr>
IMAGE_TAG=rc-1.1.0

az acr login -n $ACR_NAME
docker build -f deploy/azure_fabric/jobs/kg_export_validate_load/Dockerfile -t $ACR_NAME.azurecr.io/bank-kg-export:$IMAGE_TAG .
docker push $ACR_NAME.azurecr.io/bank-kg-export:$IMAGE_TAG
```

### Step 2 — Create Key Vault secrets (one-time)

Store:
- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`
- (optional) `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_KEY`

Terraform placeholders:
- `infra/azure/terraform/modules/keyvault/`
- `infra/azure/terraform/modules/managed_identity/`

### Step 3 — Create the Container Apps Job (one-time)

Template:
- `deploy/azure_fabric/jobs/kg_export_validate_load/job.yaml`
- `deploy/azure_fabric/jobs/kg_export_validate_load/env.template`

In production you will:
- create a Container Apps Environment
- create the Job pointing to the ACR image
- mount inputs (KGD export dir) and outputs (TTL reports)
- inject secrets from Key Vault into env vars

The job expects:
- `GOLD_DIR` = mounted path where KGD/gold extracts are available
- `OUT_DIR` = mounted output directory for TTL + reports
- `NEO4J_*` = Neo4j credentials (from Key Vault)

### Step 4 — Wire Fabric pipeline to trigger the job (recommended)

In Fabric Data Factory pipeline:

Add activities:
1. Bronze ingest
2. Silver transform (notebook)
3. Gold marts (SQL)
4. KGD refresh (SQL)

Add a Web Activity at the end:
- URL: your "job trigger" endpoint (or the Container Apps Job trigger endpoint, depending on how you implement it)
- Body: include `run_date`, `kgd_export_path`, etc.

This gives true "new data arrives → full KG refresh" automation.

### Step 5 — Set the trigger (one-time)

Choose:
- event trigger (file arrival), or
- scheduled trigger.

After this, no manual clicking is required.

---

## 7) Neo4j load + post-load checks

The job runs:
1. `kg/neo4j/cypher/00_constraints.cypher`
2. `kg/neo4j/cypher/01_n10s_init.cypher`
3. `kg/neo4j/cypher/02_import_ontology_and_data.cypher`
4. `kg/neo4j/cypher/03_validate.cypher`
5. `kg/neo4j/cypher/04_graphrag_indexes.cypher`

Import must load the validated enriched TTL artifact.

---

## 8) GraphRAG indexing job (optional)

If you also want doc indexing:
- `deploy/azure_fabric/jobs/graphrag_index/`

Steps:
1. Build/push `bank-kg-graphrag` image
2. Create Container Apps Job
3. Schedule it (daily/hourly)
4. Job runs `kg/graphrag/index_graphrag_docs.py`

---

## 9) Where the outputs are written

Recommended output locations in OneLake/ADLS:
- `.../kg/artifacts/hb_bank_data.ttl`
- `.../kg/artifacts/hb_bank_enriched.ttl`
- `.../kg/artifacts/ttl_report.json`
- `.../kg/artifacts/shacl_report.ttl`

And in Neo4j:
- constraints/indexes applied
- GraphRAG indexes created

---

## 10) Troubleshooting

- **If SHACL fails:** inspect the SHACL report (`shacl_report.ttl`) and update `kg/config/enrichment_rules.yaml` or shapes.
- **If Neo4j import fails:** check `02_import_ontology_and_data.cypher` paths and Neo4j import directory mapping.
- **If triggers don't fire:** verify Fabric trigger settings and storage/event wiring.
