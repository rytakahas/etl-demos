# 8. GraphRAG indexing specification

Schema:
- Document(doc_id, source, title, created_at)
- Chunk(chunk_id, text, embedding, as_of_date, pii_level, source_model)
- (Document)-[:HAS_CHUNK]->(Chunk)
- (Chunk)-[:MENTIONS]->(Customer|Contract|Dealer|...)

Add metadata:
- source_system, table/model, as_of_date, country, pii_level, confidence
- provenance links: chunk -> contractKey/customerKey
