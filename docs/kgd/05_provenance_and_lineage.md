# 5. Provenance & lineage

Goal: make every KG build **auditable** and **reproducible**.

## Minimum required provenance (dataset-level)
Each export emits:
- `hb:DatasetRun/<run_id>`
- `prov:startedAtTime` (UTC timestamp)
- `hb:gitCommit` (git SHA)
- `hb:sourceModel` (list of input marts/files)

Entities are linked to the run via:
- `<entity> prov:wasGeneratedBy hb:DatasetRun/<run_id>`

## Lineage across Bronze/Silver/Gold
KG instance data should be generated from **Silver/Gold** (not raw Bronze).
