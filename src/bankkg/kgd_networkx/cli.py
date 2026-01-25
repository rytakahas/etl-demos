#!/usr/bin/env python3
from __future__ import annotations

import argparse, json
from pathlib import Path
import yaml
import pandas as pd

from bankkg.kgd_networkx.spec import KGDSpec
from bankkg.kgd_networkx.build import build_graph
from bankkg.kgd_networkx.dq import run_all_checks
from bankkg.kgd_networkx.export import export_nodes_edges_csv

def main() -> None:
    ap = argparse.ArgumentParser(description="Build KGD with NetworkX from Gold CSVs.")
    ap.add_argument("--spec", required=True, help="KGD mapping yaml")
    ap.add_argument("--gold-dir", required=True, help="Gold CSV directory")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    args = ap.parse_args()

    spec = KGDSpec(**yaml.safe_load(Path(args.spec).read_text(encoding="utf-8")))
    gold_dir = Path(args.gold_dir)

    tables = {}
    for name, fname in spec.gold_sources.items():
        fp = gold_dir / fname
        if not fp.exists():
            print(f"[WARN] missing gold source: {fp}")
            tables[name] = pd.DataFrame()
        else:
            tables[name] = pd.read_csv(fp)

    G = build_graph(tables, spec)
    report = run_all_checks(G)
    paths = export_nodes_edges_csv(G, args.out_dir)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out/"dq_report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print("[OK]", report["stats"])
    print("[OK] wrote:", paths["nodes_csv"], paths["edges_csv"], out/"dq_report.json")

if __name__ == "__main__":
    main()
