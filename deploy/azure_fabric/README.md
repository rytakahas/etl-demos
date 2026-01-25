# Azure/Fabric automation: how it runs end-to-end (no manual clicking per run)

There are **two orchestration layers**:

## Layer A — Fabric pipeline (Bronze → Silver → Gold → KGD)
Runs **inside Fabric** on:
- a **schedule trigger** (hourly/daily), or
- an **event trigger** (file arrival) once configured.

**Fabric pipeline responsibilities**
1. Bronze ingest (ADLS/OneLake → Bronze tables/files)
2. Silver transforms (clean + conform types, dedupe)
3. Gold marts (warehouse SQL/dbt-style tables)
4. KGD refresh (create/refresh `kg_node_*` and `kg_edge_*`)
5. Export KGD to OneLake files (recommended) so Python jobs can read them

## Layer B — Container Apps Job (RDF export → validate → Neo4j load)
Runs your repo code **outside Fabric** as an Azure job:
- export raw TTL
- enrich TTL (config-driven)
- TTL lint + SHACL (quality gates)
- Neo4j load via n10s + cypher migrations
- post-load Neo4j DQ + GraphRAG indexes

**How it is triggered**
- Recommended: Fabric pipeline **Web Activity** calls an HTTP endpoint that triggers the Container Apps Job.
- Alternative: schedule the Container Apps Job (e.g., run 10 minutes after Gold/KGD refresh).

---

## Step-by-step Azure/Fabric setup (Ops runbook)

### 1) Fabric GitOps setup (one-time)
1. Create Fabric workspaces: dev/test/prod (or dev first).
2. Connect **dev workspace** to this repo branch (Fabric Git integration).
3. Create or import Fabric items (pipelines/notebooks/sql). See:
   - `deploy/azure_fabric/fabric/`
   - `deploy/azure_fabric/sql/`

> The repo includes placeholders/templates. In a real tenant you fill them with workspace-specific values.

### 2) Data landing (Bronze)
Raw files land in ADLS/OneLake:
- `.../bank/raw/payments/dt=YYYY-MM-DD/*.parquet`

### 3) Fabric pipeline runs Bronze→Silver→Gold→KGD
- schedule/event trigger starts pipeline automatically
- pipeline refreshes marts and KGD

### 4) KGD export handoff (recommended)
Fabric writes KGD extracts to OneLake:
- `.../Files/kgd/<run_id>/nodes/*.parquet`
- `.../Files/kgd/<run_id>/edges/*.parquet`

This becomes the stable input to the Python KG job.

### 5) Build and publish the KG runner image (one-time, CI/CD)
Runner image lives under:
- `deploy/azure_fabric/jobs/kg_export_validate_load/`

Example:
```bash
ACR_NAME=<your-acr>
IMAGE_TAG=rc-1.1.0

az acr login -n $ACR_NAME
docker build -f deploy/azure_fabric/jobs/kg_export_validate_load/Dockerfile -t $ACR_NAME.azurecr.io/bank-kg-export:$IMAGE_TAG .
docker push $ACR_NAME.azurecr.io/bank-kg-export:$IMAGE_TAG
```

### 6) Create Key Vault secrets (one-time)
Store:
- `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `NEO4J_DB`
- optional: `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_KEY`

Terraform placeholders:
- `infra/azure/terraform/modules/keyvault/`
- `infra/azure/terraform/modules/managed_identity/`

### 7) Create Container Apps Job (one-time)
Template/spec:
- `deploy/azure_fabric/jobs/kg_export_validate_load/job.yaml`
- `deploy/azure_fabric/jobs/kg_export_validate_load/env.template`

Job expects:
- `GOLD_DIR` (mounted path to KGD extracts) OR connection details to read from OneLake
- `OUT_DIR` (where TTL + reports are written)
- `NEO4J_*` creds from Key Vault

### 8) Wire Fabric → Job trigger (recommended)
In the Fabric pipeline, add a final Web Activity that calls your job trigger endpoint and passes:
- `run_id`
- `kgd_export_path` (OneLake folder)
- `out_path` (artifact folder)
- optional flags (dry run)

---

## Neo4j load & validation (what runs)
The job executes, in order:
1. `kg/neo4j/cypher/00_constraints.cypher`
2. `kg/neo4j/cypher/01_n10s_init.cypher`
3. `kg/neo4j/cypher/02_import_ontology_and_data.cypher` (imports enriched TTL)
4. `kg/neo4j/cypher/03_validate.cypher`
5. `kg/neo4j/cypher/04_graphrag_indexes.cypher`

---

## GraphRAG doc indexing (optional)
If you want doc indexing:
- `deploy/azure_fabric/jobs/graphrag_index/`

This job can be scheduled daily/hourly to:
- chunk docs → embeddings → store Document/Chunk nodes in Neo4j
- link chunks to entities
- refresh indexes if needed

---

## Outputs (recommended storage locations)

**Artifacts (OneLake/ADLS):**
- `.../kg/artifacts/<run_id>/hb_bank_data.ttl`
- `.../kg/artifacts/<run_id>/hb_bank_enriched.ttl`
- `.../kg/artifacts/<run_id>/ttl_report.json`
- `.../kg/artifacts/<run_id>/shacl_report.ttl`

**Neo4j:**
- validated graph imported
- constraints/indexes applied
- GraphRAG indexes created

---

## Local development mode (for dev only)
Use local CSV under `data/` + Neo4j Docker to validate logic.  
This is not the deployment target; it is for dev/test parity.

---

## What is still missing for fully automated Azure execution?

Your KG logic is present. What's missing is tenant-specific "execution wiring":

### Real Fabric items (not placeholders)
- actual pipeline definitions
- real notebooks
- real Warehouse SQL scripts for Gold + KGD

### A real trigger endpoint for Container Apps Job (if required by your org)
- some orgs wrap job execution behind an internal API gateway

### CI/CD that deploys Fabric assets + jobs
GitHub Actions or Azure DevOps pipeline:
- build/push ACR images
- deploy/update Container Apps Jobs
- sync Git branch → Fabric workspace and promote dev/test/prod

### Key Vault role assignments
managed identity permissions for:
- reading OneLake/ADLS
- reading Key Vault secrets
- connecting to Neo4j (network allowlist/VNet rules)

---

## Troubleshooting

- **SHACL fails**: inspect SHACL report and adjust `kg/config/enrichment_rules.yaml` first.
- **Neo4j import fails**: verify n10s config and import paths.
- **Port conflicts (local only)**: ensure only one Neo4j instance binds the host port.
