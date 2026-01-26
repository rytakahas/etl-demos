#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import time

import yaml
import pandas as pd

from bankkg.kgd_networkx.spec import KGDSpec
from bankkg.kgd_networkx.build import build_graph
from bankkg.kgd_networkx.dq import run_all_checks
from bankkg.kgd_networkx.export import export_nodes_edges_csv
from bankkg.kgd_networkx.dq_policy import load_policy, evaluate_policy
from bankkg.kgd_networkx.reports import render_markdown_report
from bankkg.kgd_networkx.history import load_prev_report, compute_drift


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def main() -> None:
    ap = argparse.ArgumentParser(description="Build KGD with NetworkX from Gold CSVs + enterprise DQ gate.")
    ap.add_argument("--spec", required=True, help="KGD mapping yaml")
    ap.add_argument("--gold-dir", required=True, help="Gold CSV directory")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--policy", default=None, help="DQ policy YAML (thresholds)")
    ap.add_argument("--prev", default=None, help="Previous dq_report.json for drift detection")
    ap.add_argument("--fail-on-violation", action="store_true", help="Exit non-zero if policy violated")
    args = ap.parse_args()

    spec = KGDSpec(**yaml.safe_load(Path(args.spec).read_text(encoding="utf-8")))
    gold_dir = Path(args.gold_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    tables = {}
    for name, fname in spec.gold_sources.items():
        fp = gold_dir / fname
        if not fp.exists():
            print(f"[WARN] missing gold source: {fp}")
            tables[name] = pd.DataFrame()
        else:
            tables[name] = pd.read_csv(fp)

    G = build_graph(tables, spec)

    checks = run_all_checks(G, supernode_threshold=5000)
    metrics = checks["metrics"]

    policy = load_policy(args.policy)
    ok, violations = evaluate_policy(metrics, policy)

    prev = load_prev_report(args.prev)
    drift = compute_drift(prev, metrics)

    paths = export_nodes_edges_csv(G, str(out))

    dq_report = {
        "run_meta": {
            "run_id": f"{int(time.time())}",
            "run_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "git_sha": git_sha(),
        },
        "metrics": metrics,
        "checks": {
            "supernodes": checks.get("supernodes", []),
            "node_labels_top": metrics.get("node_labels_top", []),
            "edge_types_top": metrics.get("edge_types_top", []),
        },
        "policy_ok": ok,
        "violations": [v.__dict__ for v in violations],
        "drift": drift,
        "outputs": paths,
    }

    (out / "dq_report.json").write_text(json.dumps(dq_report, indent=2, default=str), encoding="utf-8")
    md = render_markdown_report(dq_report["run_meta"], metrics, dq_report["checks"], ok, violations, drift=drift)
    (out / "dq_report.md").write_text(md, encoding="utf-8")

    print("[OK] wrote:", out / "dq_report.json", out / "dq_report.md")
    print("[OK] nodes:", metrics["nodes"], "edges:", metrics["edges"], "policy_ok:", ok)

    if args.fail_on_violation and not ok:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
