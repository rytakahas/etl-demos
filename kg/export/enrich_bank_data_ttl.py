#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from decimal import Decimal

import yaml
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD

DEFAULT_BASE_IRI = "https://example.org/honda-bank/kg#"

XSD_MAP = {
    "xsd:string": XSD.string,
    "xsd:integer": XSD.integer,
    "xsd:decimal": XSD.decimal,
    "xsd:date": XSD.date,
    "xsd:dateTime": XSD.dateTime,
}

def dt(x: str):
    return XSD_MAP.get(x, XSD.string)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="kg/config/enrichment_rules.yaml")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    base_iri = cfg.get("base_iri", DEFAULT_BASE_IRI)
    HB = Namespace(base_iri)

    g = Graph()
    g.parse(args.in_path, format="turtle")

    patched = {}

    for rule in cfg.get("rules", []):
        rtype = rule["type"]
        name = rule.get("name", rtype)
        patched[name] = 0

        if rtype == "ensure_node":
            cls = rule["class"]
            rid = rule["id"]
            node = HB[f"{cls}/{rid}"]
            g.add((node, RDF.type, HB[cls]))
            for pred, spec in rule.get("literals", {}).items():
                g.add((node, HB[pred], Literal(spec["value"], datatype=dt(spec["datatype"]))))

        elif rtype == "ensure_object":
            subj_cls = rule["subject_class"]
            pred = rule["predicate"]
            obj_cls = rule["object_class"]
            obj_id = rule["object_id"]
            obj = HB[f"{obj_cls}/{obj_id}"]
            g.add((obj, RDF.type, HB[obj_cls]))

            for s in g.subjects(RDF.type, HB[subj_cls]):
                if not any(g.objects(s, HB[pred])):
                    g.add((s, HB[pred], obj))
                    patched[name] += 1

        elif rtype == "ensure_datatype_alias":
            subj_cls = rule["subject_class"]
            tgt = rule["target_predicate"]
            sources = rule.get("source_predicates", [])
            datatype = dt(rule.get("datatype", "xsd:string"))
            default = rule.get("default")

            for s in g.subjects(RDF.type, HB[subj_cls]):
                if any(g.objects(s, HB[tgt])):
                    continue

                val = None
                for sp in sources:
                    val = next(g.objects(s, HB[sp]), None)
                    if val is not None:
                        break

                if val is None and default is not None:
                    if datatype == XSD.decimal:
                        val = Literal(Decimal(str(default)), datatype=datatype)
                    elif datatype == XSD.integer:
                        val = Literal(int(default), datatype=datatype)
                    else:
                        val = Literal(str(default), datatype=datatype)

                if val is not None:
                    g.add((s, HB[tgt], val))
                    patched[name] += 1

        elif rtype == "ensure_datatype_default":
            subj_cls = rule["subject_class"]
            pred = rule["predicate"]
            datatype = dt(rule.get("datatype", "xsd:string"))
            default = rule.get("default", "")

            for s in g.subjects(RDF.type, HB[subj_cls]):
                if any(g.objects(s, HB[pred])):
                    continue
                g.add((s, HB[pred], Literal(default, datatype=datatype)))
                patched[name] += 1

        else:
            raise ValueError(f"Unknown rule type: {rtype}")

    # Atomic write
    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    g.serialize(destination=str(tmp), format="turtle")
    tmp.replace(out_path)

    print(f"[ENRICH] wrote: {out_path}")
    for k, v in patched.items():
        print(f"[ENRICH] {k}: {v}")

if __name__ == "__main__":
    main()

