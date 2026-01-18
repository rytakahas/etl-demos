// kg/neo4j/cypher/02_import_ontology_and_data.cypher
// IMPORTANT: Use absolute paths. In this setup, file:///hb_bank.ttl resolves to /hb_bank.ttl (not the import dir).

// 1) Import ontology (use onto import)
CALL n10s.onto.import.fetch(
  "file:///var/lib/neo4j/import/hb_bank.ttl",
  "Turtle"
) YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN "ONTOLOGY" AS kind, terminationStatus, triplesLoaded, triplesParsed, extraInfo;

// 2) Import data (instances)
CALL n10s.rdf.import.fetch(
  "file:///var/lib/neo4j/import/hb_bank_data.ttl",
  "Turtle"
) YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN "DATA" AS kind, terminationStatus, triplesLoaded, triplesParsed, extraInfo;
