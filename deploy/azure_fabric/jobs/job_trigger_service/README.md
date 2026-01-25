# Job Trigger Service (optional)

This is a small HTTP service intended to be called from **Fabric Data Factory Web Activity**.
It triggers an external runner (typically an **Azure Container Apps Job**) with parameters (run_id, paths).

Why this exists:
- Some orgs prefer Fabric to call a *stable internal endpoint* (API Gateway), not Azure control-plane APIs directly.
- You can enforce allowlists, auth, logging, and idempotency.

## What it does
- `POST /trigger/kg-export` with JSON payload:
  - `run_id`
  - `kgd_export_path`
  - `out_path`
- Validates payload against allowlist rules
- Starts the runner (placeholder in this template)

## Production notes
- Use Managed Identity for Azure calls.
- Read subscription/resource group/job name from env.
- Validate inputs with strict allowlists.
- Record an audit log per trigger request.
