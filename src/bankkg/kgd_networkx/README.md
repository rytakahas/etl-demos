# NetworkX KGD builder

This module builds deterministic KGD (nodes/edges) from Gold tables using a YAML spec,
runs graph DQ checks, and exports CSV files for Neo4j load.

## Run
```bash
PYTHONPATH=src python -m bankkg.kgd_networkx.cli \
  --spec src/bankkg/kgd_networkx/kgspec.example.yaml \
  --gold-dir data \
  --out-dir out/kgd
```

Outputs:
- `out/kgd/nodes.csv`
- `out/kgd/edges.csv`
- `out/kgd/dq_report.json`
