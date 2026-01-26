# Job Trigger Service (production-ready)

This service is called from **Fabric Data Factory Web Activity**.
It starts an **Azure Container Apps Job** execution via Azure Resource Manager REST.

## Endpoints
- `GET /health`
- `POST /trigger/kg-export`

## Required env vars
- `ACA_SUBSCRIPTION_ID`
- `ACA_RESOURCE_GROUP`
- `ACA_JOB_NAME`

## Optional env vars
- `ACA_API_VERSION` (default: 2025-07-01)
- `ALLOWED_KGD_PREFIX` (default: onelake://)
- `ALLOWED_OUT_PREFIX` (default: onelake://)
- `APPLY_ENV_OVERRIDES` (default: 1)
- `RUN_ID_ENV` (default: RUN_ID)
- `KGD_EXPORT_PATH_ENV` (default: KGD_EXPORT_PATH)
- `OUT_PATH_ENV` (default: OUT_PATH)

## Identity / permissions
Run this service as a Container App with **Managed Identity**.
Grant that identity **Contributor** on the Container Apps Job resource:
`/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.App/jobs/<jobName>`
