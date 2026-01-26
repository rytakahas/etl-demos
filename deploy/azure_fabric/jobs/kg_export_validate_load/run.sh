#!/usr/bin/env bash
set -euo pipefail

echo "[JOB] Export → Enrich → Validate → Load Neo4j"

: "${GOLD_DIR:?Set GOLD_DIR}"
: "${OUT_DIR:?Set OUT_DIR}"
: "${NEO4J_URI:?Set NEO4J_URI}"
: "${NEO4J_USER:?Set NEO4J_USER}"
: "${NEO4J_PASSWORD:?Set NEO4J_PASSWORD}"
: "${NEO4J_DB:?Set NEO4J_DB}"

mkdir -p "$OUT_DIR"

python kg/export/export_bank_kg_data_ttl.py --data-dir "$GOLD_DIR" --out "$OUT_DIR/hb_bank_data.ttl"

python kg/export/enrich_bank_data_ttl.py   --config kg/config/enrichment_rules.yaml   --in "$OUT_DIR/hb_bank_data.ttl"   --out "$OUT_DIR/hb_bank_enriched.ttl"

python kg/validation/validate_ttl.py   --in kg/ontology/hb_bank.ttl "$OUT_DIR/hb_bank_enriched.ttl"   --out "$OUT_DIR/ttl_report.json"

python kg/validation/validate_shacl.py   --data "$OUT_DIR/hb_bank_enriched.ttl"   --shapes kg/shacl/hb_bank.shapes.ttl   --ontology kg/ontology/hb_bank.ttl   --inference rdfs   --out "$OUT_DIR/shacl_report.ttl"

python - <<'PY'
import os
from pathlib import Path
from neo4j import GraphDatabase

uri = os.environ["NEO4J_URI"]
user = os.environ["NEO4J_USER"]
pwd  = os.environ["NEO4J_PASSWORD"]
db   = os.environ.get("NEO4J_DB","neo4j")

cypher_files = [
    "kg/neo4j/cypher/00_constraints.cypher",
    "kg/neo4j/cypher/01_n10s_init.cypher",
    "kg/neo4j/cypher/02_import_ontology_and_data.cypher",
    "kg/neo4j/cypher/03_validate.cypher",
    "kg/neo4j/cypher/04_graphrag_indexes.cypher",
]

def split_semicolon(text: str):
    return [s.strip() for s in text.split(";") if s.strip()]

driver = GraphDatabase.driver(uri, auth=(user, pwd))
try:
    with driver.session(database=db) as sess:
        for fp in cypher_files:
            p = Path(fp)
            if not p.exists():
                raise FileNotFoundError(fp)
            for stmt in split_semicolon(p.read_text(encoding="utf-8")):
                sess.run(stmt).consume()
    print("[JOB] Neo4j load complete")
finally:
    driver.close()
PY

echo "[JOB] DONE"
