from __future__ import annotations

import argparse
import csv
import json
import os
from typing import List, Tuple, Dict, Any

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Neo4j quick schema + counts report")
    p.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    p.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"))
    p.add_argument("--password", default=os.getenv("NEO4J_PASSWORD", "Password123!"))
    p.add_argument("--database", default=os.getenv("NEO4J_DATABASE", "neo4j"))

    p.add_argument("--out-csv", default="", help="Optional CSV path for label counts")
    p.add_argument("--schema-json", default="", help="Optional JSON path for db.schema.visualization() output")
    p.add_argument("--limit-labels", type=int, default=0, help="Optional: only process first N labels")

    return p.parse_args()


def get_labels(sess) -> List[str]:
    return [r["label"] for r in sess.run("CALL db.labels() YIELD label RETURN label ORDER BY label")]


def count_nodes_for_label(sess, label: str) -> int:
    # label comes from db.labels(), so safe to interpolate as an identifier.
    cypher = f"MATCH (n:`{label}`) RETURN count(n) AS c"
    return int(sess.run(cypher).single()["c"])


def get_reltype_counts(sess) -> List[Tuple[str, int]]:
    rows = sess.run(
        """
        MATCH ()-[r]->()
        RETURN type(r) AS relType, count(*) AS c
        ORDER BY c DESC
        """
    )
    return [(r["relType"], int(r["c"])) for r in rows]


def export_schema_visualization(sess) -> Dict[str, Any]:
    """
    Returns a JSON-serializable dict of Neo4j schema visualization.
    The result typically includes two collections: nodes + relationships.
    """
    rec = sess.run("CALL db.schema.visualization()").single()
    if rec is None:
        return {"nodes": [], "relationships": []}

    # neo4j returns Node/Relationship objects; convert to serializable shapes
    nodes_out: List[Dict[str, Any]] = []
    rels_out: List[Dict[str, Any]] = []

    # The record keys are usually 'nodes' and 'relationships'
    nodes = rec.get("nodes", []) or []
    rels = rec.get("relationships", []) or []

    for n in nodes:
        # n is a neo4j.graph.Node
        nodes_out.append(
            {
                "id": str(n.id),
                "labels": list(n.labels),
                "properties": dict(n.items()),
            }
        )

    for r in rels:
        # r is a neo4j.graph.Relationship
        rels_out.append(
            {
                "id": str(r.id),
                "type": r.type,
                "start_node_id": str(r.start_node.id),
                "end_node_id": str(r.end_node.id),
                "properties": dict(r.items()),
            }
        )

    return {"nodes": nodes_out, "relationships": rels_out}


def main() -> None:
    args = parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    try:
        driver.verify_connectivity()
        print(f"Connected OK: uri={args.uri} db={args.database}")

        with driver.session(database=args.database) as sess:
            # Labels + counts
            labels = get_labels(sess)
            if args.limit_labels and args.limit_labels > 0:
                labels = labels[: args.limit_labels]

            label_counts: List[Tuple[str, int]] = []
            for label in labels:
                c = count_nodes_for_label(sess, label)
                label_counts.append((label, c))

            label_counts.sort(key=lambda x: x[1], reverse=True)

            print("\nNode counts by label:")
            for label, c in label_counts:
                print(f"{label:30s} {c}")

            # Relationship type counts
            rel_counts = get_reltype_counts(sess)
            print("\nRelationship counts by type:")
            for rel, c in rel_counts:
                print(f"{rel:30s} {c}")

            # Schema-ish (property tables)
            print("\nNode type properties (db.schema.nodeTypeProperties):")
            for r in sess.run("CALL db.schema.nodeTypeProperties()"):
                print(r.data())

            print("\nRelationship type properties (db.schema.relTypeProperties):")
            for r in sess.run("CALL db.schema.relTypeProperties()"):
                print(r.data())

            # Optional: write CSV label counts
            if args.out_csv:
                os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
                with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["label", "count"])
                    w.writerows(label_counts)
                print(f"\nWrote CSV: {args.out_csv}")

            # Optional: write schema visualization JSON
            if args.schema_json:
                os.makedirs(os.path.dirname(args.schema_json) or ".", exist_ok=True)
                schema = export_schema_visualization(sess)
                payload = {
                    "uri": args.uri,
                    "database": args.database,
                    "generated_by": "neo4j_counts.py",
                    "schema_visualization": schema,
                }
                with open(args.schema_json, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                print(f"\nWrote schema JSON: {args.schema_json}")

    except Neo4jError as e:
        print("Neo4j error:", e)
        raise
    finally:
        driver.close()


if __name__ == "__main__":
    main()

