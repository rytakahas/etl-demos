# Fabric Web Activity → Trigger Service (example)

Add a **Web Activity** at the end of your Fabric pipeline.

## URL
`https://<trigger-service-fqdn>/trigger/kg-export`

## Method
`POST`

## Headers
`Content-Type: application/json`

## Body
```json
{
  "run_id": "@{pipeline().RunId}",
  "kgd_export_path": "onelake://<workspace>/<lakehouse>/Files/kgd/@{formatDateTime(utcnow(),'yyyy-MM-dd')}/",
  "out_path": "onelake://<workspace>/<lakehouse>/Files/kg_artifacts/@{formatDateTime(utcnow(),'yyyy-MM-dd')}/"
}
```

## Notes
- Ensure the trigger service identity can start the Container Apps Job.
- The service overrides job execution env vars so each run is parameterized.
