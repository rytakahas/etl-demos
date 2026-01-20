// Input: $chunk_ids (from vector/fulltext retrieval step)
MATCH (c:Chunk)
WHERE c.chunk_id IN $chunk_ids
OPTIONAL MATCH (c)-[:MENTIONS]->(e)
RETURN
  c.chunk_id,
  c.text,
  collect(labels(e))[0..10] AS linked_entity_labels
LIMIT 50;
