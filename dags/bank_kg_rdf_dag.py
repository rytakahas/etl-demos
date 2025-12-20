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

# Neo4j from inside Airflow container should be reached via host.docker.internal
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://host.docker.internal:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "Password123!")

REPO_ROOT = Path("/usr/local/airflow")
CYPHER_DIR = REPO_ROOT / "kg" / "neo4j" / "cypher"


def wait_for_neo4j(timeout_s: int = 90) -> None:
    """Wait until Neo4j Bolt is reachable + auth works."""
    deadline = time.time() + timeout_s
    last_err = None

    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            driver.close()
            print("Neo4j connectivity OK")
            return
        except Exception as e:
            last_err = e
            time.sleep(3)

    raise RuntimeError(f"Neo4j not ready after {timeout_s}s. Last error: {last_err!r}")


def run_cypher_file(path: Path, *, ignore_n10s_nonempty: bool = False) -> None:
    cypher = path.read_text(encoding="utf-8")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session(database="neo4j") as sess:
            statements = [s.strip() for s in cypher.split(";") if s.strip()]
            for stmt in statements:
                try:
                    sess.run(stmt).consume()
                except ClientError as e:
                    msg = str(e)
                    code = getattr(e, "code", None)

                    # Make n10s init idempotent: if graph is not empty, skip init and continue.
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
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session(database="neo4j") as sess:
        nodes = sess.run("MATCH (n) RETURN count(n) AS n").single()["n"]
        rels = sess.run("MATCH ()-[r]->() RETURN count(r) AS n").single()["n"]
        rel_types = [
            r["t"]
            for r in sess.run(
                "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t ORDER BY t LIMIT 50"
            )
        ]
    driver.close()

    if nodes == 0 or rels == 0:
        raise RuntimeError(
            f"KG validation failed: nodes={nodes}, rels={rels}, rel_types={rel_types}"
        )

    out = CYPHER_DIR / "ready_to_visualize.cypher"
    out.write_text("MATCH p=(a)-[r]->(b)\nRETURN p\nLIMIT 50;\n", encoding="utf-8")
    print(f"validation ok: nodes={nodes}, rels={rels}, rel_types={rel_types}")
    print(f"wrote {out}")


default_args = {
    "retries": 5,
    "retry_delay": timedelta(seconds=20),
}

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

    export_ttl = BashOperator(
        task_id="export_ttl",
        cwd=str(REPO_ROOT),
        bash_command=(
            "python kg/export/export_bank_kg_data_ttl.py "
            "--data-dir data "
            "--out kg/neo4j/import/hb_bank_data.ttl"
        ),
    )

    copy_ontology = BashOperator(
        task_id="copy_ontology_to_import",
        cwd=str(REPO_ROOT),
        bash_command="cp -f kg/ontology/hb_bank.ttl kg/neo4j/import/hb_bank.ttl",
    )

    wait_neo4j = PythonOperator(
        task_id="wait_neo4j",
        python_callable=wait_for_neo4j,
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

    dbt_build >> export_ttl >> copy_ontology >> wait_neo4j >> neo4j_constraints >> neo4j_n10s_init >> neo4j_import >> neo4j_validate

