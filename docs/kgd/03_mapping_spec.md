# 3. Mapping specification (DWH → KG)

This doc defines the **contract** between the warehouse/lakehouse and the KG.

## 3.1 Source-of-truth layers
- **Silver**: conformed identifiers, deduped entities, stable types
- **Gold**: business grain marts (facts/dims) for consistent semantics

**Rule of thumb:** KG instance data should be emitted from **Silver/Gold**, not Bronze.

## 3.2 Mapping table
See: `docs/kgd/03_mapping_table.csv`

**TODO:** Update table names/columns to match your dbt models exactly.

## 3.3 Mapping conventions
- **Entity tables (dimensions)** → KG **classes** (nodes)
- **Fact rows** → either relationships (binary) or event nodes (n-ary)

## 3.4 Key constraints
Every entity uses a stable key property (e.g., `hb:contractKey`) and a stable IRI.

## 3.5 Required relationships
- Contract **must** link to Customer (`minCount=1`)
- DefaultEvent **must** link to Contract (`minCount=1`)

Reflect these in SHACL + dbt tests.
