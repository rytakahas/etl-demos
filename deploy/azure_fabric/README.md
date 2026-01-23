# Azure / Microsoft Fabric production runbook (client tenant)

This runbook describes how to deploy the same pipeline in a **client tenant**.

You keep the logic (dbt models, ontology, SHACL rules, Cypher) in Git and map runtime services:

- Orchestration: Airflow DAG → Fabric Data Factory / Azure Data Factory pipelines
- Storage: local folders → OneLake / ADLS Gen2
- Warehouse: DuckDB → Fabric Warehouse (or Snowflake)
- KG validation: SHACL job
- Graph: Neo4j + n10s

---

## 1) Container registry (ACR)

Authenticate:
```bash
az login
az acr login --name <ACR_NAME>
```

Docs:
```text
https://learn.microsoft.com/en-us/cli/azure/acr?view=azure-cli-latest
https://learn.microsoft.com/en-us/azure/container-registry/container-registry-authentication
```

Build + push (example):
```bash
ACR_LOGIN_SERVER="<acr>.azurecr.io"
docker build -f docker/Dockerfile.airflow -t ${ACR_LOGIN_SERVER}/bankkg-airflow:0.1.0 .
docker push ${ACR_LOGIN_SERVER}/bankkg-airflow:0.1.0
```

---

## 2) Azure Artifacts (private python feeds) — optional

If the client uses Azure DevOps Artifacts feeds:
```text
https://learn.microsoft.com/en-us/azure/devops/artifacts/python/project-setup-python?view=azure-devops
https://learn.microsoft.com/en-us/azure/devops/artifacts/quickstarts/python-packages?view=azure-devops
```

---

## 3) Fabric Data Factory pipelines

Create pipelines that mirror the DAG stages:

1) Bronze ingest (batch + optional streaming)
2) Silver/Gold transforms (SQL notebooks or dbt job)
3) Export TTL (notebook/container job)
4) SHACL validate (notebook/container job)
5) Neo4j import (container job / AKS job)

Docs:
```text
https://learn.microsoft.com/en-us/fabric/data-factory/
https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-overview
https://learn.microsoft.com/en-us/fabric/data-factory/tutorial-end-to-end-pipeline
```

---

## 4) Azure ML pipelines (optional)

If you add ML scoring/training, use AML pipelines.

Docs:
```text
https://learn.microsoft.com/en-us/azure/machine-learning/tutorial-pipeline-python-sdk?view=azureml-api-2
https://learn.microsoft.com/en-us/azure/machine-learning/reference-yaml-job-pipeline?view=azureml-api-2
```

A minimal YAML example is in:
- `deploy/azure_fabric/aml/pipeline_job.yml`

---

## 5) Neo4j deployment notes

Production options:
- AKS
- VM
- Container Apps

Must-haves:
- persistent storage for `/data`
- mounted import directory for `/var/lib/neo4j/import`
- n10s procedures allowlist/unrestricted
