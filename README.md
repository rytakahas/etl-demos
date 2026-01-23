# etl-demos — Banking Lakehouse → DWH → Semantic KG → Neo4j GraphRAG

This repository demonstrates a **production-style banking analytics + Knowledge Graph** pipeline:

- **Bronze / Silver / Gold** data modeling (dbt)
- **Semantic layer**: OWL/Turtle ontology + SHACL validation
- **Graph DB**: Neo4j + n10s (Neosemantics) RDF import
- Optional **GraphRAG** indexing (chunks + embeddings + vector/fulltext indexes)

Two modes:

1) **Local (no accounts / no credit card):** Docker Compose stack you can run on a laptop.
2) **Azure / Microsoft Fabric (client tenant):** Runbook + infra skeleton so you can deploy in a client environment.

---

## Recommended branching

Create a branch for Azure/Fabric deployment docs and local compose, so you don’t break your working graph branch:

```bash
git checkout feat/graphdb
git checkout -b feat/azure-fabric-prod
```

Then unzip this pack into the repo root and commit.

---

## Repository layout (DDD + TDD friendly)

```
src/bankkg/                 # Domain-first package (DDD-style)
  domain/                   # Entities, value objects, invariants
  application/              # Use-cases (export KG, validate, load)
  infrastructure/           # Adapters (rdf export, neo4j load)

dbt/                        # Silver/Gold models + tests
kg/
  ontology/                 # OWL/Turtle ontology
  shacl/                    # SHACL shapes (semantic quality rules)
  export/                   # Export scripts (marts -> TTL)
  neo4j/cypher/             # n10s init + import + validation Cypher

dags/                       # Airflow DAGs (local orchestration)
deploy/azure_fabric/        # Client-tenant runbook (Fabric/ADF/AML/ACR)
infra/azure/terraform/      # Optional IaC skeleton

tests/                      # Pytest (TDD): unit + smoke tests
```

---

## Local run (100% laptop)

### Start services

```bash
docker compose -f docker-compose.local.yml up --build
```

- Airflow: `http://localhost:8080` (admin/admin)
- Neo4j Browser: `http://localhost:7474` (neo4j/password)

### Run the pipeline

Trigger these DAGs:
- `bank_etl_dag`
- `bank_kg_rdf_load`

Verify in Neo4j:

```cypher
MATCH (n) RETURN count(n) AS nodes;
MATCH ()-[r]->() RETURN count(r) AS rels;
```

---

## Azure / Microsoft Fabric (client tenant)

See `deploy/azure_fabric/README.md`.

References (official docs):
```text
ACR (Azure Container Registry):
- https://learn.microsoft.com/en-us/cli/azure/acr?view=azure-cli-latest
- https://learn.microsoft.com/en-us/azure/container-registry/container-registry-authentication

Azure Artifacts (Python feeds):
- https://learn.microsoft.com/en-us/azure/devops/artifacts/python/project-setup-python?view=azure-devops
- https://learn.microsoft.com/en-us/azure/devops/artifacts/quickstarts/python-packages?view=azure-devops

Fabric Data Factory:
- https://learn.microsoft.com/en-us/fabric/data-factory/
- https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-overview

Azure ML pipelines:
- https://learn.microsoft.com/en-us/azure/machine-learning/tutorial-pipeline-python-sdk?view=azureml-api-2
- https://learn.microsoft.com/en-us/azure/machine-learning/reference-yaml-job-pipeline?view=azureml-api-2
```
