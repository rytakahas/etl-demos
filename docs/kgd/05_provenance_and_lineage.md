# 5. Provenance & lineage

Goal: make every KG build **auditable** and **reproducible**.

---

## 5.1 Minimum required provenance (dataset-level)

Each KG export MUST include a “dataset run” record:

- `run_id` (stable identifier for the export run)
- `generated_at` (UTC timestamp)
- `git_commit` (short SHA or release tag)
- `source_models` (dbt models or source files used)
- (optional but recommended) `as_of_date` / data window

### How it is represented in RDF (current implementation)

We emit a run node:

- `hb:DatasetRun/<run_id>`

and attach metadata:

- `prov:startedAtTime` (xsd:dateTime)
- `hb:gitCommit` (xsd:string)
- `hb:sourceModel` (repeated for each model/file)

We then link exported entities to the run:

- `<entity> prov:wasGeneratedBy hb:DatasetRun/<run_id>`

This enables audit questions like:
- “Which pipeline run produced this node?”
- “Which git version generated this KG export?”

---

## 5.2 Optional provenance (record-level)

If needed for stricter audit/compliance, add per-entity fields:

- `hb:sourceSystem` (e.g., CoreBanking, Basikon, CRM)
- `hb:sourceModel` / `hb:sourceTable`
- `hb:sourcePrimaryKey`
- `hb:loadedAt` (timestamp)

These can be added as literals on each entity node, or via a dedicated provenance node per entity.

---

## 5.3 Lineage across Bronze/Silver/Gold

- Bronze: immutable raw ingests for audit/reprocessing
- Silver: conformed IDs + cleaned records
- Gold: business grain marts (facts/dims) used for consistent semantics

**KG instance data must originate from Silver/Gold**.

dbt documentation (`dbt docs`) serves as the relational lineage artifact. The KG run record should reference dbt model names (or materialized table names) so lineage can be traced end-to-end.

---

## 5.4 Operational checklist

- [ ] Exporter emits `hb:DatasetRun/<run_id>`
- [ ] Exporter writes `prov:startedAtTime`, `hb:gitCommit`, `hb:sourceModel`
- [ ] Exporter links entities with `prov:wasGeneratedBy`
- [ ] Run ID is captured in logs (Airflow task logs / CI logs)
