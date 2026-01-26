i#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple

from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, OWL


def parse_ttl(path: Path) -> Graph:
    g = Graph()
    g.parse(str(path), format="turtle")
    return g


def _subjects_of_type(g: Graph, t) -> Set[str]:
    return {str(s) for s in g.subjects(RDF.type, t)}


def _has_label(g: Graph, s) -> bool:
    return any(True for _ in g.objects(s, RDFS.label))


def main() -> None:
    ap = argparse.ArgumentParser(description="TTL parse + lint + basic ontology drift checks.")
    ap.add_argument("--in", dest="inputs", nargs="+", required=True, help="Input TTL files. Put ontology first.")
    ap.add_argument("--out", default="kg/export/ttl_report.json", help="Output JSON report path.")
    ap.add_argument("--base", default="https://example.org/honda-bank/kg#", help="Base namespace for drift checks.")
    args = ap.parse_args()

    paths = [Path(p) for p in args.inputs]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    # Parse
    graphs: List[Graph] = []
    for p in paths:
        if not p.exists():
            errors.append({"level": "error", "code": "MISSING_FILE", "message": "Input file missing", "subject": str(p)})
            continue
        try:
            graphs.append(parse_ttl(p))
        except Exception as e:
            errors.append({"level": "error", "code": "PARSE_ERROR", "message": str(e), "subject": str(p)})

    ok = len(errors) == 0
    if not ok:
        report = {"ok": False, "inputs": [str(p) for p in paths], "base": args.base, "stats": {}, "errors": errors, "warnings": warnings}
        out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        raise SystemExit(1)

    # Combine graphs for stats
    combined = Graph()
    for g in graphs:
        for triple in g:
            combined.add(triple)

    # Treat first file as ontology (best-effort); rest as data
    onto = graphs[0]
    data = Graph()
    for g in graphs[1:]:
        for t in g:
            data.add(t)

    HB = Namespace(args.base)

    # Stats
    classes = _subjects_of_type(onto, OWL.Class) | _subjects_of_type(onto, RDFS.Class)
    obj_props = _subjects_of_type(onto, OWL.ObjectProperty)
    dt_props = _subjects_of_type(onto, OWL.DatatypeProperty)
    individuals = _subjects_of_type(combined, OWL.NamedIndividual)  # often 0; fine

    stats = {
        "triples": len(combined),
        "classes": len(classes),
        "object_properties": len(obj_props),
        "data_properties": len(dt_props),
        "individuals": len(individuals),
    }

    # Ontology hygiene warnings
    for s in list(classes) + list(obj_props) + list(dt_props):
        # missing label
        ss = None
        for subj in onto.subjects(None, None):
            if str(subj) == s:
                ss = subj
                break
        if ss is not None and not _has_label(onto, ss):
            warnings.append({"level": "warning", "code": "MISSING_LABEL", "message": "Entity missing rdfs:label", "subject": s})

    # Domain/range for object properties
    for s in obj_props:
        ss = None
        for subj in onto.subjects(None, None):
            if str(subj) == s:
                ss = subj
                break
        if ss is None:
            continue
        if not any(True for _ in onto.objects(ss, RDFS.domain)):
            warnings.append({"level": "warning", "code": "MISSING_DOMAIN", "message": "ObjectProperty missing rdfs:domain", "subject": s})
        if not any(True for _ in onto.objects(ss, RDFS.range)):
            warnings.append({"level": "warning", "code": "MISSING_RANGE", "message": "ObjectProperty missing rdfs:range", "subject": s})

    # Schema drift: predicates used in data in the base namespace but not declared in ontology as OWL property
    if len(graphs) >= 2:
        declared_props = set()
        for p in onto.subjects(RDF.type, OWL.ObjectProperty):
            declared_props.add(str(p))
        for p in onto.subjects(RDF.type, OWL.DatatypeProperty):
            declared_props.add(str(p))
        # allow annotation properties too
        for p in onto.subjects(RDF.type, OWL.AnnotationProperty):
            declared_props.add(str(p))

        for pred in set(str(p) for p in data.predicates()):
            if pred.startswith(args.base) and pred not in declared_props:
                warnings.append({
                    "level": "warning",
                    "code": "UNDECLARED_PREDICATE",
                    "message": "Predicate used but not declared as an OWL property (schema drift?)",
                    "subject": pred,
                })

    report = {
        "ok": True,
        "inputs": [str(p) for p in paths],
        "base": args.base,
        "stats": stats,
        "errors": [],
        "warnings": warnings,
    }

    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

