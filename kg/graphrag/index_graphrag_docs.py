#!/usr/bin/env python3
"""
Index unstructured documents into Neo4j for GraphRAG-style retrieval.

Creates:
  (:Document {doc_id, source, title})
  (:Chunk {chunk_id, text, embedding})
  (:Document)-[:HAS_CHUNK]->(:Chunk)
  (:Chunk)-[:MENTIONS]->(:Customer|:Contract|...)   (optional, via simple rules)

Embeddings:
- Default: SentenceTransformers all-MiniLM-L6-v2 (384 dims)
- If you prefer OpenAI embeddings, adapt `embed_texts()`.

Usage:
  pip install neo4j sentence-transformers numpy
  python kg/graphrag/index_graphrag_docs.py --uri neo4j://localhost:7687 --user neo4j --password password --docs ./docs

Then run the Cypher in: kg/neo4j/cypher/04_graphrag_indexes.cypher
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from typing import List, Tuple

import numpy as np
from neo4j import GraphDatabase

try:
    from sentence_transformers import SentenceTransformer
except Exception as e:  # pragma: no cover
    SentenceTransformer = None  # type: ignore


def chunk_text(text: str, max_chars: int = 1200, overlap: int = 150) -> List[str]:
    text = text.strip()
    if not text:
        return []
    chunks = []
    i = 0
    while i < len(text):
        j = min(len(text), i + max_chars)
        chunks.append(text[i:j])
        i = max(0, j - overlap)
        if j == len(text):
            break
    return chunks


def embed_texts(texts: List[str], model_name: str = "sentence-transformers/all-MiniLM-L6-v2") -> List[List[float]]:
    if SentenceTransformer is None:
        raise RuntimeError("sentence-transformers is not installed. Run: pip install sentence-transformers")
    model = SentenceTransformer(model_name)
    vecs = model.encode(texts, show_progress_bar=False, normalize_embeddings=True)
    return [v.astype(np.float32).tolist() for v in vecs]


def naive_entity_linking(chunk: str) -> List[Tuple[str, str]]:
    """
    Extremely simple heuristic linker.
    Returns list of (label, key_value) pairs.
    Example patterns:
      - Customer C12345
      - Contract K-2024-0001
    Replace with real NER + entity resolution in production.
    """
    out = []
    for token in chunk.replace(",", " ").replace(";", " ").split():
        if token.startswith("C") and token[1:].isdigit():
            out.append(("Customer", token))
        if token.upper().startswith("K-"):
            out.append(("Contract", token))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True, help="Neo4j URI, e.g., neo4j://localhost:7687")
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--docs", required=True, help="Folder containing .txt/.md/.pdf (text only handled here)")
    ap.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    ap.add_argument("--max-chars", type=int, default=1200)
    ap.add_argument("--overlap", type=int, default=150)
    ap.add_argument("--link-entities", action="store_true", help="Enable naive entity linking to domain nodes")
    args = ap.parse_args()

    docs_dir = Path(args.docs)
    paths = sorted([p for p in docs_dir.rglob("*") if p.suffix.lower() in {".txt", ".md"}])
    if not paths:
        raise SystemExit(f"No .txt/.md documents found under: {docs_dir}")

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))

    # Prepare all chunks
    doc_rows = []
    chunk_rows = []
    mention_rows = []

    for p in paths:
        text = p.read_text(encoding="utf-8", errors="ignore")
        doc_id = hashlib.sha1(str(p).encode("utf-8")).hexdigest()[:16]
        title = p.name
        doc_rows.append({"doc_id": doc_id, "source": str(p), "title": title})

        chunks = chunk_text(text, max_chars=args.max_chars, overlap=args.overlap)
        if not chunks:
            continue

        embs = embed_texts(chunks, model_name=args.model)
        for k, (ch, emb) in enumerate(zip(chunks, embs)):
            chunk_id = f"{doc_id}:{k}"
            chunk_rows.append({"doc_id": doc_id, "chunk_id": chunk_id, "text": ch, "embedding": emb})

            if args.link_entities:
                for label, key in naive_entity_linking(ch):
                    mention_rows.append({"chunk_id": chunk_id, "label": label, "key": key})

    with driver.session() as s:
        # Documents
        s.run(
            """
            UNWIND $rows AS r
            MERGE (d:Document {doc_id: r.doc_id})
            SET d.source = r.source,
                d.title  = r.title
            """,
            rows=doc_rows,
        )

        # Chunks
        s.run(
            """
            UNWIND $rows AS r
            MATCH (d:Document {doc_id: r.doc_id})
            MERGE (c:Chunk {chunk_id: r.chunk_id})
            SET c.text = r.text,
                c.embedding = r.embedding
            MERGE (d)-[:HAS_CHUNK]->(c)
            """,
            rows=chunk_rows,
        )

        # Mentions (optional; expects domain nodes keyed by e.g. customerKey / contractKey properties)
        if mention_rows:
            s.run(
                """
                UNWIND $rows AS r
                MATCH (c:Chunk {chunk_id: r.chunk_id})
                CALL {
                  WITH r, c
                  WITH r, c
                  WHERE r.label = 'Customer'
                  MATCH (e:Customer {customerKey: r.key})
                  MERGE (c)-[:MENTIONS]->(e)
                  RETURN 1 AS ok
                  UNION
                  WITH r, c
                  WHERE r.label = 'Contract'
                  MATCH (e:Contract {contractKey: r.key})
                  MERGE (c)-[:MENTIONS]->(e)
                  RETURN 1 AS ok
                }
                """,
                rows=mention_rows,
            )

    driver.close()
    print(f"Indexed {len(doc_rows)} documents and {len(chunk_rows)} chunks into Neo4j.")


if __name__ == "__main__":
    main()
