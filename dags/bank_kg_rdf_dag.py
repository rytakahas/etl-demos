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
from neo4j.exceptions import ClientError, AuthError


# -----------------------------------------------------------------------------
# Neo4j connection (Astro Neo4j in docker-compose.override.yml maps host 7688 -> container 7687)
# -----------------------------------------------------------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://host.docker.internal:7688")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "Password123!")  # MUST match NEO4J_AUTH in compose
NEO4J_DATABASE = os.getenv("NEO4J_DB", "neo4j")               # use NEO4J_DB consistently

# Repo paths inside Astro containers
REPO_ROOT = Path("/usr/local/airflow")
CYPHER_DIR = REPO_ROOT / "kg" / "neo4j" / "cypher"

# Artifacts written for Neo4j import (these paths map to /var/lib/neo4j/import via compose bind mount)
RAW_TTL = "kg/neo4j/import/hb_bank_data.ttl"
ENR_TTL = "kg/neo4j/import/hb_bank_enriched.ttl"
ONTO_TTL = "kg/neo4j/import/hb_bank.ttl"

# SHACL shapes (keep legacy default for compatibility; you can switch to hb_bank.shapes.ttl if you want)
SHAPES_TTL = os.getenv("SHAPES_TTL", "kg/shacl/bank_shapes.ttl")

# Optional: NetworkX DQ gate (enable only if deps exist in Airflow image)
ENABLE_NETWORKX_DQ = os.getenv("ENABLE_NETWORKX_DQ", "0") == "1"
NETWORKX_SPEC = os.getenv("NETWORKX_SPEC", "src/bankkg/kgd_networkx/kgspec.example.yaml")
NETWORKX_GOLD_DIR = os.getenv("NETWORKX_GOLD_DIR", "data")
NETWORKX_OUT_DIR = os.getenv("NETWORKX_OUT_DIR", "kg/export/kgd_out")
NETWORKX_POLICY = os.getenv("NETWORKX_POLICY", "kg/config/kgd_policy.yaml")


def neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def wait_for_neo4j(timeout_s: int = 300) -> None:
    """Wait until Neo4j Bolt is reachable + auth works (fail fast on auth errors)."""
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

        except AuthError as e:
            # FAIL FAST: wrong password/user (avoid lockouts)
            raise RuntimeError(
                f"Neo4j auth failed: uri={NEO4J_URI} user={NEO4J_USER} db={NEO4J_DATABASE}"
            ) from e

        except Exception as e:
            last_err = f"{type(e).__name__} code={getattr(e,'code',None)} msg={str(e)}"
            print(f"Neo4j not ready yet: {last_err}")
            time.sleep(5)

    raise RuntimeError(f"Neo4j not ready after {timeout_s}s. Last error: {last_err}")


def run_cypher_file(path: Path, *, ignore_n10s_nonempty: bool = False) -> None:
    """Execute a .cypher file, splitting on semicolons; prints returned rows."""
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

                    # Make n10s init idempotent
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
    """Post-load operational validation: ensure graph is non-empty and provide a helper query."""
    driver = neo4j_driver()
    try:
        with driver.session(database=NEO4J_DATABASE) as sess:
            nodes = sess.run("MATCH (n) RETURN count(n) AS n").single()["n"]
            rels = sess.run("MATCH ()-[r]->() RETURN count(r) AS n").single()["n"]
            rel_types = [
                r["t"]
                for r in sess.run(
                    "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t ORDER BY t LIMIT 50"
                )
            ]
    finally:
        driver.close()

    if nodes == 0 or rels == 0:
        raise RuntimeError(
            f"KG validation failed: nodes={nodes}, rels={rels}, rel_types={rel_types}"
        )

    out = CYPHER_DIR / "ready_to_visualize.cypher"
    out.write_text("MATCH p=(a)-[r]->(b)\nRETURN p\nLIMIT 50;\n", encoding="utf-8")
    print(f"validation ok: nodes={nodes}, rels={rels}, rel_types={rel_types}")
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

    # 1) Build warehouse models + tests (dbt build = run + test)
    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=str(REPO_ROOT / "dbt"),
        bash_command="dbt build --profiles-dir .",
    )

    # 1.5) Optional: NetworkX DQ gate (pre-load analytics checks)
    # Enable by setting ENABLE_NETWORKX_DQ=1 and ensuring deps are in requirements.airflow.txt
    if ENABLE_NETWORKX_DQ:
        networkx_dq_gate = BashOperator(
            task_id="networkx_dq_gate",
            cwd=str(REPO_ROOT),
            bash_command=(
                f"PYTHONPATH=src python -m bankkg.kgd_networkx.cli "
                f"--spec {NETWORKX_SPEC} "
                f"--gold-dir {NETWORKX_GOLD_DIR} "
                f"--out-dir {NETWORKX_OUT_DIR} "
                f"--policy {NETWORKX_POLICY} "
                f"--fail-on-violation"
            ),
        )
    else:
        networkx_dq_gate = None

    # 2) Export RDF data graph (raw TTL)
    export_ttl = BashOperator(
        task_id="export_ttl",
        cwd=str(REPO_ROOT),
        bash_command=(
            "mkdir -p kg/neo4j/import && chmod 777 kg/neo4j/import && "
            "python kg/export/export_bank_kg_data_ttl.py "
            "--data-dir data "
            f"--out {RAW_TTL}"
        ),
    )

    # 2.5) Enrich (config-driven) -> enriched TTL
    enrich_ttl = BashOperator(
        task_id="enrich_ttl",
        cwd=str(REPO_ROOT),
        bash_command=(
            "python kg/export/enrich_bank_data_ttl.py "
            "--config kg/config/enrichment_rules.yaml "
            f"--in  {RAW_TTL} "
            f"--out {ENR_TTL}"
        ),
    )

    # 3) SHACL validate ENRICHED TTL (quality gate)
    shacl_validate = BashOperator(
        task_id="shacl_validate",
        cwd=str(REPO_ROOT),
        bash_command=(
            "python -m pyshacl "
            f"-s {SHAPES_TTL} "
            f"-d {ENR_TTL}"
        ),
    )

    # 4) Copy ontology into Neo4j import folder (so n10s can import via file:///var/lib/neo4j/import/...)
    copy_ontology = BashOperator(
        task_id="copy_ontology_to_import",
        cwd=str(REPO_ROOT),
        bash_command=(
            "mkdir -p kg/neo4j/import && chmod 777 kg/neo4j/import && "
            f"cp -f kg/ontology/hb_bank.ttl {ONTO_TTL}"
        ),
    )

    # 5) Wait for Neo4j
    wait_neo4j = PythonOperator(
        task_id="wait_neo4j",
        python_callable=wait_for_neo4j,
        op_kwargs={"timeout_s": 300},
    )

    # 6) Apply constraints (keys/uniqueness)
    neo4j_constraints = PythonOperator(
        task_id="neo4j_constraints",
        python_callable=lambda: run_cypher_file(CYPHER_DIR / "00_constraints.cypher"),
    )

    # 7) Initialize n10s config (idempotent)
    neo4j_n10s_init = PythonOperator(
        task_id="neo4j_n10s_init",
        python_callable=lambda: run_cypher_file(
            CYPHER_DIR / "01_n10s_init.cypher", ignore_n10s_nonempty=True
        ),
    )

    # 8) Import ontology + enriched data
    neo4j_import = PythonOperator(
        task_id="neo4j_import",
        python_callable=lambda: run_cypher_file(CYPHER_DIR / "02_import_ontology_and_data.cypher"),
    )

    # 9) Validate graph and generate a helper query for visualization
    neo4j_validate = PythonOperator(
        task_id="neo4j_validate",
        python_callable=validate_graph_and_write_visualize,
    )

    # Chain
    if networkx_dq_gate:
        (
            dbt_build
            >> networkx_dq_gate
            >> export_ttl
            >> enrich_ttl
            >> shacl_validate
            >> copy_ontology
            >> wait_neo4j
            >> neo4j_constraints
            >> neo4j_n10s_init
            >> neo4j_import
            >> neo4j_validate
        )
    else:
        (
            dbt_build
            >> export_ttl
            >> enrich_ttl
            >> shacl_validate
            >> copy_ontology
            >> wait_neo4j
            >> neo4j_constraints
            >> neo4j_n10s_init
            >> neo4j_import
            >> neo4j_validate
        )
