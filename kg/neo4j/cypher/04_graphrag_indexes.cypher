// 04_graphrag_indexes.cypher
// Create indexes for GraphRAG-style retrieval (vector + full-text).
// Run after your KG is loaded.

CREATE VECTOR INDEX chunk_embedding IF NOT EXISTS
FOR (c:Chunk)
ON (c.embedding)
OPTIONS {indexConfig: {`vector.dimensions`: 384, `vector.similarity_function`: 'cosine'}};

CREATE FULLTEXT INDEX chunk_text IF NOT EXISTS
FOR (c:Chunk)
ON EACH [c.text];

CREATE INDEX doc_id IF NOT EXISTS
FOR (d:Document) ON (d.doc_id);

CREATE INDEX chunk_id IF NOT EXISTS
FOR (c:Chunk) ON (c.chunk_id);
