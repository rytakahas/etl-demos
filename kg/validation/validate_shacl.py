#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rdflib import Graph
from pyshacl import validate


def load_ttl(path: Path) -> Graph:
    g = Graph()
    g.parse(path, format="turtle")
    return g


def main() -> None:
    ap = argparse.ArgumentParser(description="Run SHACL validation (pySHACL) on Turtle data.")
    ap.add_argument("--data", required=True, help="Data TTL file (instances)")
    ap.add_argument("--shapes", required=True, help="Shapes TTL file")
    ap.add_argument("--ontology", default="", help="Ontology TTL (optional, but recommended)")
    ap.add_argument("--inference", default="rdfs", choices=["none", "rdfs", "owlrl", "both"])
    ap.add_argument("--out", default="", help="Write report graph TTL to this path (optional)")
    args = ap.parse_args()

    data_path = Path(args.data)
    shapes_path = Path(args.shapes)
    onto_path = Path(args.ontology) if args.ontology else None

    for p in [data_path, shapes_path] + ([onto_path] if onto_path else []):
        if p and not p.exists():
            print(f"[ERROR] Missing file: {p}", file=sys.stderr)
            sys.exit(1)

    try:
        data_g = load_ttl(data_path)
        shapes_g = load_ttl(shapes_path)
        onto_g = load_ttl(onto_path) if onto_path else None
    except Exception as e:
        print(f"[ERROR] TTL parse failed: {e}", file=sys.stderr)
        sys.exit(1)

    inference = None if args.inference == "none" else args.inference

    conforms, report_graph, report_text = validate(
        data_graph=data_g,
        shacl_graph=shapes_g,
        ont_graph=onto_g,
        inference=inference,
        abort_on_first=False,
        allow_infos=True,
        allow_warnings=True,
        meta_shacl=False,
        advanced=True,
        js=False,
        debug=False,
    )

    print(report_text)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        report_graph.serialize(destination=str(out_path), format="turtle")
        print(f"[OK] Wrote SHACL report to: {out_path}")

    sys.exit(0 if conforms else 1)


if __name__ == "__main__":
    main()

