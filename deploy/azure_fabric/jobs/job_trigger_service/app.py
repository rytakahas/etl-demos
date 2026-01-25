from __future__ import annotations

import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Fabric Job Trigger Service")

ACA_JOB_NAME = os.getenv("ACA_JOB_NAME", "bank-kg-export-validate-load")
ACA_RESOURCE_GROUP = os.getenv("ACA_RESOURCE_GROUP", "")
ACA_SUBSCRIPTION_ID = os.getenv("ACA_SUBSCRIPTION_ID", "")

ALLOWED_KGD_PREFIX = os.getenv("ALLOWED_KGD_PREFIX", "onelake://")
ALLOWED_OUT_PREFIX = os.getenv("ALLOWED_OUT_PREFIX", "onelake://")

class TriggerRequest(BaseModel):
    run_id: str
    kgd_export_path: str
    out_path: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/trigger/kg-export")
def trigger(req: TriggerRequest):
    if not req.kgd_export_path.startswith(ALLOWED_KGD_PREFIX):
        raise HTTPException(status_code=400, detail="kgd_export_path not allowed")
    if not req.out_path.startswith(ALLOWED_OUT_PREFIX):
        raise HTTPException(status_code=400, detail="out_path not allowed")

    if not ACA_RESOURCE_GROUP or not ACA_SUBSCRIPTION_ID:
        raise HTTPException(status_code=500, detail="ACA_RESOURCE_GROUP/ACA_SUBSCRIPTION_ID not configured")

    # Placeholder: start an Azure Container Apps Job execution.
    # Implement using Azure SDK/REST with Managed Identity.
    return {
        "status": "accepted",
        "job": ACA_JOB_NAME,
        "run_id": req.run_id,
        "kgd_export_path": req.kgd_export_path,
        "out_path": req.out_path,
        "note": "Implement ACA job start via SDK/REST"
    }
