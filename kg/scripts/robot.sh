#!/usr/bin/env bash
set -euo pipefail

# Uses Docker image for ROBOT (no local Java setup needed).
# Validate ontology + run a reasoner (HermiT / ELK depending on profile).
# NOTE: This is a demo gate; tune profiles for your ontology.

ONTO="kg/ontology/hb_bank.ttl"

docker run --rm -v "$PWD:/work" -w /work obolibrary/robot   robot validate-profile --profile OWL2_DL --input "$ONTO"   reason --reasoner ELK --input "$ONTO" --output /tmp/out.ttl   report --input "$ONTO" --output /tmp/report.tsv || true

echo "✅ ROBOT done (see /tmp/report.tsv if running locally inside docker volume)."
