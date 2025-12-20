MATCH (n) RETURN count(n) AS nodes;
MATCH ()-[r]->() RETURN count(r) AS rels;
MATCH ()-[r]->() RETURN DISTINCT type(r) AS relType ORDER BY relType LIMIT 50;
