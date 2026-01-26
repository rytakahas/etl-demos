from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.identity import DefaultAzureCredential

app = FastAPI(title="Fabric → Container Apps Job Trigger Service")

ACA_SUBSCRIPTION_ID = os.getenv("ACA_SUBSCRIPTION_ID", "")
ACA_RESOURCE_GROUP = os.getenv("ACA_RESOURCE_GROUP", "")
ACA_JOB_NAME = os.getenv("ACA_JOB_NAME", "")

ACA_API_VERSION = os.getenv("ACA_API_VERSION", "2025-07-01")
AZURE_CLOUD_RESOURCE = os.getenv("AZURE_CLOUD_RESOURCE", "https://management.azure.com")

ALLOWED_KGD_PREFIX = os.getenv("ALLOWED_KGD_PREFIX", "onelake://")
ALLOWED_OUT_PREFIX = os.getenv("ALLOWED_OUT_PREFIX", "onelake://")

APPLY_ENV_OVERRIDES = os.getenv("APPLY_ENV_OVERRIDES", "1") == "1"
RUN_ID_ENV = os.getenv("RUN_ID_ENV", "RUN_ID")
KGD_PATH_ENV = os.getenv("KGD_EXPORT_PATH_ENV", "KGD_EXPORT_PATH")
OUT_PATH_ENV = os.getenv("OUT_PATH_ENV", "OUT_PATH")


class TriggerRequest(BaseModel):
    run_id: str
    kgd_export_path: str
    out_path: str


@app.get("/health")
def health():
    return {"status": "ok"}


def _token() -> str:
    cred = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    scope = f"{AZURE_CLOUD_RESOURCE}/.default"
    return cred.get_token(scope).token


def _arm_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_token()}",
        "Content-Type": "application/json",
    }


def _job_resource_url() -> str:
    if not (ACA_SUBSCRIPTION_ID and ACA_RESOURCE_GROUP and ACA_JOB_NAME):
        raise HTTPException(
            status_code=500,
            detail="Missing ACA_SUBSCRIPTION_ID/ACA_RESOURCE_GROUP/ACA_JOB_NAME env vars",
        )
    return (
        f"{AZURE_CLOUD_RESOURCE}/subscriptions/{ACA_SUBSCRIPTION_ID}"
        f"/resourceGroups/{ACA_RESOURCE_GROUP}"
        f"/providers/Microsoft.App/jobs/{ACA_JOB_NAME}"
        f"?api-version={ACA_API_VERSION}"
    )


def _job_start_url() -> str:
    return (
        f"{AZURE_CLOUD_RESOURCE}/subscriptions/{ACA_SUBSCRIPTION_ID}"
        f"/resourceGroups/{ACA_RESOURCE_GROUP}"
        f"/providers/Microsoft.App/jobs/{ACA_JOB_NAME}/start"
        f"?api-version={ACA_API_VERSION}"
    )


def _get_job_template() -> Dict[str, Any]:
    r = requests.get(_job_resource_url(), headers=_arm_headers(), timeout=30)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Failed to GET job: {r.status_code} {r.text}")
    body = r.json()
    tmpl = body.get("properties", {}).get("template")
    if not isinstance(tmpl, dict):
        raise HTTPException(status_code=500, detail="Job template missing in GET response")
    return tmpl


def _upsert_env(env_list: Optional[List[Dict[str, Any]]], name: str, value: str) -> List[Dict[str, Any]]:
    env_list = list(env_list or [])
    found = False
    for e in env_list:
        if e.get("name") == name:
            e["value"] = value
            e.pop("secretRef", None)
            found = True
            break
    if not found:
        env_list.append({"name": name, "value": value})
    return env_list


@app.post("/trigger/kg-export")
def trigger(req: TriggerRequest):
    if not req.kgd_export_path.startswith(ALLOWED_KGD_PREFIX):
        raise HTTPException(status_code=400, detail="kgd_export_path not allowed")
    if not req.out_path.startswith(ALLOWED_OUT_PREFIX):
        raise HTTPException(status_code=400, detail="out_path not allowed")

    payload: Dict[str, Any] = {}

    if APPLY_ENV_OVERRIDES:
        tmpl = _get_job_template()
        containers = tmpl.get("containers", [])
        init_containers = tmpl.get("initContainers", [])

        if not containers:
            raise HTTPException(status_code=500, detail="Job template has no containers")

        for c in containers:
            c["env"] = _upsert_env(c.get("env"), RUN_ID_ENV, req.run_id)
            c["env"] = _upsert_env(c.get("env"), KGD_PATH_ENV, req.kgd_export_path)
            c["env"] = _upsert_env(c.get("env"), OUT_PATH_ENV, req.out_path)

        payload = {"containers": containers}
        if init_containers:
            payload["initContainers"] = init_containers

    r = requests.post(_job_start_url(), headers=_arm_headers(), json=payload or {}, timeout=60)

    if r.status_code not in (200, 202):
        raise HTTPException(status_code=502, detail=f"Failed to start job: {r.status_code} {r.text}")

    location = r.headers.get("Location")
    return {
        "status": "started",
        "job": ACA_JOB_NAME,
        "run_id": req.run_id,
        "kgd_export_path": req.kgd_export_path,
        "out_path": req.out_path,
        "location": location,
        "note": "Use Location header (if present) to poll job execution status.",
    }
