#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

from rdflib import Graph, URIRef
from rdflib.namespace import RDF, RDFS, OWL


BASE_DEFAULT = "https://example.org/honda-bank/kg#"


@dataclass
class Issue:
    level: str          # "error" | "warning"
    code: str           # short machine code
    message: str        # human message
    subject: str | None = None


def _uri_str(u) -> str:
    return str(u) if u is not None else ""


def parse_ttl(files: List[Path]) -> Graph:
    g = Graph()
    for f in files:
        g.parse(f, format="turtle")
    return g


def find_kg_uris(g: Graph, base: str) -> Set[URIRef]:
    out: Set[URIRef] = set()
    for s, p, o in g:
        for term in (s, p, o):
            if isinstance(term, URIRef) and str(term).startswith(base):
                out.add(term)
    return out


def check_spaces_in_localnames(g: Graph, base: str) -> List[Issue]:
    issues: List[Issue] = []
    for u in find_kg_uris(g, base):
        frag = str(u)[len(base):]
        if " " in frag or "'" in frag:
            issues.append(Issue(
                level="error",
                code="KG_LOCALNAME_BAD",
                message=f"KG IRI local name contains space/quote: {frag}",
                subject=str(u),
            ))
    return issues


def check_domain_range(g: Graph) -> List[Issue]:
    issues: List[Issue] = []
    # ObjectProperties and DatatypeProperties should ideally have domain+range
    for prop_type, label in [(OWL.ObjectProperty, "ObjectProperty"), (OWL.DatatypeProperty, "DatatypeProperty")]:
        props = set(g.subjects(RDF.type, prop_type))
        for p in props:
            has_domain = any(True for _ in g.objects(p, RDFS.domain))
            has_range = any(True for _ in g.objects(p, RDFS.range))
            if not has_domain:
                issues.append(Issue(
                    level="warning",
                    code="MISSING_DOMAIN",
                    message=f"{label} missing rdfs:domain",
                    subject=_uri_str(p),
                ))
            if not has_range:
                issues.append(Issue(
                    level="warning",
                    code="MISSING_RANGE",
                    message=f"{label} missing rdfs:range",
                    subject=_uri_str(p),
                ))
    return issues


def check_labels(g: Graph) -> List[Issue]:
    issues: List[Issue] = []

    def needs_label(u: URIRef) -> bool:
        return not any(True for _ in g.objects(u, RDFS.label))

    # Classes + properties
    candidates: Set[URIRef] = set()
    candidates |= set(g.subjects(RDF.type, OWL.Class))
    candidates |= set(g.subjects(RDF.type, OWL.ObjectProperty))
    candidates |= set(g.subjects(RDF.type, OWL.DatatypeProperty))

    for u in candidates:
        if needs_label(u):
            issues.append(Issue(
                level="warning",
                code="MISSING_LABEL",
                message="Entity missing rdfs:label",
                subject=_uri_str(u),
            ))
    return issues


def check_undefined_predicates(g: Graph, base: str) -> List[Issue]:
    """
    If your DATA TTL uses kg:predicates that are not declared in the ontology,
    this flags them. (Common source of silent schema drift.)
    """
    issues: List[Issue] = []

    # collect predicates in KG namespace
    preds: Set[URIRef] = set()
    for _, p, _ in g:
        if isinstance(p, URIRef) and str(p).startswith(base):
            preds.add(p)

    # declared properties
    declared: Set[URIRef] = set()
    declared |= set(g.subjects(RDF.type, OWL.ObjectProperty))
    declared |= set(g.subjects(RDF.type, OWL.DatatypeProperty))
    declared |= set(g.subjects(RDF.type, OWL.AnnotationProperty))

    # if predicates exist that are not declared at all
    for p in sorted(preds, key=str):
        if p not in declared:
            issues.append(Issue(
                level="warning",
                code="UNDECLARED_PREDICATE",
                message="Predicate used but not declared as an OWL property (schema drift?)",
                subject=_uri_str(p),
            ))
    return issues


def stats(g: Graph) -> Dict[str, int]:
    return {
        "triples": len(g),
        "classes": len(set(g.subjects(RDF.type, OWL.Class))),
        "object_properties": len(set(g.subjects(RDF.type, OWL.ObjectProperty))),
        "data_properties": len(set(g.subjects(RDF.type, OWL.DatatypeProperty))),
        "individuals": len(set(g.subjects(RDF.type, OWL.NamedIndividual))),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse + lint TTL (schema + data).")
    ap.add_argument("--base", default=BASE_DEFAULT, help="KG base IRI (namespace), default Honda Bank demo base.")
    ap.add_argument("--in", dest="inputs", nargs="+", required=True, help="One or more TTL files to parse.")
    ap.add_argument("--out", default="", help="Write JSON report to file (optional).")
    ap.add_argument("--strict", action="store_true", help="Fail on warnings too (exit code 2).")
    args = ap.parse_args()

    files = [Path(p) for p in args.inputs]
    for f in files:
        if not f.exists():
            print(f"[ERROR] Missing file: {f}", file=sys.stderr)
            sys.exit(1)

    try:
        g = parse_ttl(files)
    except Exception as e:
        report = {
            "ok": False,
            "error": f"TTL parse failed: {e}",
            "inputs": [str(f) for f in files],
        }
        out_json = json.dumps(report, indent=2)
        if args.out:
            Path(args.out).write_text(out_json, encoding="utf-8")
        print(out_json)
        sys.exit(1)

    issues: List[Issue] = []
    issues += check_spaces_in_localnames(g, args.base)
    issues += check_domain_range(g)
    issues += check_labels(g)
    issues += check_undefined_predicates(g, args.base)

    n_errors = sum(1 for x in issues if x.level == "error")
    n_warnings = sum(1 for x in issues if x.level == "warning")

    report = {
        "ok": n_errors == 0 and (not args.strict or n_warnings == 0),
        "inputs": [str(f) for f in files],
        "base": args.base,
        "stats": stats(g),
        "errors": [asdict(i) for i in issues if i.level == "error"],
        "warnings": [asdict(i) for i in issues if i.level == "warning"],
    }

    out_json = json.dumps(report, indent=2)
    if args.out:
        Path(args.out).write_text(out_json, encoding="utf-8")
    print(out_json)

    if n_errors > 0:
        sys.exit(1)
    if args.strict and n_warnings > 0:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()

