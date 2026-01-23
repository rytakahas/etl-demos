CALL n10s.onto.import.fetch(
  "file:///var/lib/neo4j/import/hb_bank.ttl",
  "Turtle"
) YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN "ONTOLOGY" AS kind, terminationStatus, triplesLoaded, triplesParsed, extraInfo;

CALL n10s.rdf.import.fetch(
  "file:///var/lib/neo4j/import/hb_bank_enriched.ttl",
  "Turtle"
) YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN "DATA_ENRICHED" AS kind, terminationStatus, triplesLoaded, triplesParsed, extraInfo;
