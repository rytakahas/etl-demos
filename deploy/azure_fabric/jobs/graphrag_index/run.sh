#!/usr/bin/env bash
set -euo pipefail

: "${NEO4J_URI:?}"
: "${NEO4J_USER:?}"
: "${NEO4J_PASSWORD:?}"
: "${DOCS_DIR:?}"

python kg/graphrag/index_graphrag_docs.py   --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASSWORD"   --docs "$DOCS_DIR"
