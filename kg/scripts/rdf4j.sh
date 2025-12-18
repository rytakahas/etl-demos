#!/usr/bin/env bash
set -euo pipefail

# Start RDF4J server + workbench
docker rm -f rdf4j-hb >/dev/null 2>&1 || true
docker run -d --name rdf4j-hb -p 8080:8080 eclipse/rdf4j-workbench:latest

echo "✅ RDF4J Workbench: http://localhost:8080/rdf4j-workbench"
echo "Create a repository, then upload TTL via the UI or via HTTP."
