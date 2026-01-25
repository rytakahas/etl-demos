from __future__ import annotations
import os
from neo4j import GraphDatabase
from typing import Dict, Any, List

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "Password123")
NEO4J_DB = os.getenv("NEO4J_DB", "neo4j")

def _driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def get_customer_profile(customer_id: str) -> Dict[str, Any]:
    """
    Minimal profile tool. Replace Cypher with your real labels/props.
    Returns evidence paths so the LLM can cite them.
    """
    q_nodes = """
    MATCH (c:Customer {customerKey:$cid})
    OPTIONAL MATCH (c)<-[:hasCustomer]-(ct:Contract)
    OPTIONAL MATCH (ct)<-[:paymentForContract]-(p:Payment)
    RETURN
      count(DISTINCT ct) AS contracts,
      count(DISTINCT p)  AS payments
    """
    evidence_paths = [f"(Customer/{customer_id})<-[:hasCustomer]-(Contract)-[:paymentForContract]<-(Payment)"]

    with _driver() as d, d.session(database=NEO4J_DB) as s:
        rec = s.run(q_nodes, cid=customer_id).single()
        if not rec:
            return {"customer_id": customer_id, "contracts": 0, "payments": 0, "evidence_paths": []}
        return {
            "customer_id": customer_id,
            "contracts": rec["contracts"],
            "payments": rec["payments"],
            "evidence_paths": evidence_paths,
        }

def retrieve_policy_chunks(query: str, k: int = 3) -> List[Dict[str, Any]]:
    """
    Placeholder retrieval: if you store policy docs as (:Chunk) nodes, implement fulltext/vector query here.
    For now returns a static example.
    """
    return [
        {"chunk_id": "policy::chunk::1", "source": "policy_v1", "text": "DTI must be <= 35% for prime tier."}
    ][:k]

