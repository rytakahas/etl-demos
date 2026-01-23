# dags/bank_kg_rdf_dag.py
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

# -----------------------------------------------------------------------------
# Neo4j connection (Neo4j Docker on Mac: host 7688->container 7687)
# -----------------------------------------------------------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://host.docker.internal:7688")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "Password123")
NEO4J_DATABASE = os.getenv("NEO4J_DB", "neo4j")

REPO_ROOT = Path("/usr/local/airflow")
CYPHER_DIR = REPO_ROOT / "kg" / "neo4j" / "cypher"

# Canonical artifact locations (Neo4j mounts ../ontology -> /var/lib/neo4j/import)
RAW_TTL = "kg/ontology/hb_bank_data.ttl"
ENR_TTL = "kg/ontology/hb_bank_enriched.ttl"
ONTO_TTL = "kg/ontology/hb_bank.ttl"
SHAPES_TTL = "kg/shacl/hb_bank.shapes.ttl"  # or keep bank_shapes.ttl if you prefer


def neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def wait_for_neo4j(timeout_s: int = 300) -> None:
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            driver = neo4j_driver()
            driver.verify_connectivity()
            with driver.session(database=NEO4J_DATABASE) as sess:
                sess.run("RETURN 1 AS ok").single()
            driver.close()
            print(f"Neo4j connectivity OK: uri={NEO4J_URI}, db={NEO4J_DATABASE}")
            return
        except Exception as e:
            last_err = repr(e)
            print(f"Neo4j not ready yet: {last_err}")
            time.sleep(5)
    raise RuntimeError(f"Neo4j not ready after {timeout_s}s. Last error: {last_err}")


def run_cypher_file(path: Path, *, ignore_n10s_nonempty: bool = False) -> None:
    cypher = path.read_text(encoding="utf-8")
    statements = [s.strip() for s in cypher.split(";") if s.strip()]
    driver = neo4j_driver()
    try:
        with driver.session(database=NEO4J_DATABASE) as sess:
            for stmt in statements:
                try:
                    res = sess.run(stmt)
                    rows = res.data()
                    if rows:
                        print(f"[Cypher result] {path.name} :: {rows[:5]}")
                    res.consume()
                except ClientError as e:
                    msg = str(e)
                    code = getattr(e, "code", None)
                    if (
                        ignore_n10s_nonempty
                        and "n10s.graphconfig.init" in stmt
                        and (
                            "The graph is non-empty" in msg
                            or "Config cannot be changed" in msg
                            or "GraphConfigException" in msg
                        )
                    ):
                        print(f"Skipping n10s init (already configured / graph not empty). code={code}")
                        continue
                    raise
    finally:
        driver.close()


def validate_graph_and_write_visualize() -> None:
    driver = neo4j_driver()
    try:
        with driver.session(database=NEO4J_DATABASE) as sess:
            nodes = sess.run("MATCH (n) RETURN count(n) AS n").single()["n"]
            rels = sess.run("MATCH ()-[r]->() RETURN count(r) AS n").single()["n"]
    finally:
        driver.close()

    if nodes == 0 or rels == 0:
        raise RuntimeError(f"KG validation failed: nodes={nodes}, rels={rels}")

    out = CYPHER_DIR / "ready_to_visualize.cypher"
    out.write_text("MATCH p=(a)-[r]->(b)\nRETURN p\nLIMIT 50;\n", encoding="utf-8")
    print(f"validation ok: nodes={nodes}, rels={rels}")
    print(f"wrote {out}")


default_args = {"retries": 5, "retry_delay": timedelta(seconds=20)}

with DAG(
    dag_id="bank_kg_rdf_load",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kg", "neo4j", "rdf", "n10s"],
    default_args=default_args,
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=str(REPO_ROOT / "dbt"),
        bash_command="dbt build --profiles-dir .",
    )

    # Export RAW TTL to kg/ontology (this is what Neo4j imports via /var/lib/neo4j/import)
    export_ttl = BashOperator(
        task_id="export_ttl",
        cwd=str(REPO_ROOT),
        bash_command=(
            f"python kg/export/export_bank_kg_data_ttl.py "
            f"--data-dir data "
            f"--out {RAW_TTL}"
        ),
    )

    # Enrich using your YAML-driven rules (replaces patch scripts for generalization)
    enrich_ttl = BashOperator(
        task_id="enrich_ttl",
        cwd=str(REPO_ROOT),
        bash_command=(
            f"python kg/export/enrich_bank_data_ttl.py "
            f"--config kg/config/enrichment_rules.yaml "
            f"--in {RAW_TTL} "
            f"--out {ENR_TTL}"
        ),
    )

    # SHACL validate the enriched TTL (quality gate)
    shacl_validate = BashOperator(
        task_id="shacl_validate",
        cwd=str(REPO_ROOT),
        bash_command=(
            f"python kg/validation/validate_shacl.py "
            f"--data {ENR_TTL} "
            f"--shapes {SHAPES_TTL} "
            f"--ontology {ONTO_TTL} "
            f"--inference rdfs "
            f"--out kg/export/shacl_report.ttl"
        ),
    )

    wait_neo4j = PythonOperator(
        task_id="wait_neo4j",
        python_callable=wait_for_neo4j,
        op_kwargs={"timeout_s": 300},
    )

    neo4j_constraints = PythonOperator(
        task_id="neo4j_constraints",
        python_callable=lambda: run_cypher_file(CYPHER_DIR / "00_constraints.cypher"),
    )

    neo4j_n10s_init = PythonOperator(
        task_id="neo4j_n10s_init",
        python_callable=lambda: run_cypher_file(CYPHER_DIR / "01_n10s_init.cypher", ignore_n10s_nonempty=True),
    )

    neo4j_import = PythonOperator(
        task_id="neo4j_import",
        python_callable=lambda: run_cypher_file(CYPHER_DIR / "02_import_ontology_and_data.cypher"),
    )

    neo4j_validate = PythonOperator(
        task_id="neo4j_validate",
        python_callable=validate_graph_and_write_visualize,
    )

    (
        dbt_build
        >> export_ttl
        >> enrich_ttl
        >> shacl_validate
        >> wait_neo4j
        >> neo4j_constraints
        >> neo4j_n10s_init
        >> neo4j_import
        >> neo4j_validate
    )

