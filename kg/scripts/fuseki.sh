#!/usr/bin/env bash
set -euo pipefail

# Start Fuseki
docker rm -f fuseki-hb >/dev/null 2>&1 || true
docker run -d --name fuseki-hb -p 3030:3030 stain/jena-fuseki

echo "✅ Fuseki running: http://localhost:3030"
echo "Create dataset 'pekg' in the UI (or preconfigure). Then load TTL:"
echo "  curl -X POST -H 'Content-Type: text/turtle' --data-binary @data/kg/hb_bank.ttl http://localhost:3030/pekg/data"
echo ""
echo "Example query endpoint:"
echo "  http://localhost:3030/pekg/sparql"
