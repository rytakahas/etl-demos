from fastapi import FastAPI
from pydantic import BaseModel
from neo4j import GraphDatabase
import os

NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USERNAME = os.environ["NEO4J_USERNAME"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
app = FastAPI(title="KG Loader")

class Triple(BaseModel):
    subject: str
    predicate: str
    obj: str
    subject_type: str = "Entity"
    object_type: str = "Entity"

class UpsertPayload(BaseModel):
    triples: list[Triple]
    source: str | None = None

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/upsert")
def upsert(payload: UpsertPayload):
    q = """
    UNWIND $triples AS t
    MERGE (s:Entity {id: t.subject})
      ON CREATE SET s.type = t.subject_type
    MERGE (o:Entity {id: t.obj})
      ON CREATE SET o.type = t.object_type
    CALL apoc.merge.relationship(s, t.predicate, {}, {}, o) YIELD rel
    RETURN count(*) AS n
    """
    # If you don't have APOC installed in this local neo4j, replace with dynamic rel types via CASE or fixed rels.
    # For a no-plugin version, use a single rel type :REL with property predicate=t.predicate.

    # Safer no-APOC variant:
    q_no_apoc = """
    UNWIND $triples AS t
    MERGE (s:Entity {id: t.subject})
      ON CREATE SET s.type = t.subject_type
    MERGE (o:Entity {id: t.obj})
      ON CREATE SET o.type = t.object_type
    MERGE (s)-[r:REL {predicate: t.predicate}]->(o)
    RETURN count(*) AS n
    """

    with driver.session(database="neo4j") as session:
        n = session.run(q_no_apoc, triples=[t.model_dump() for t in payload.triples]).single()["n"]
    return {"upserted": n}

