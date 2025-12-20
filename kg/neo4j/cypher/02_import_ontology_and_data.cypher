CALL n10s.rdf.import.fetch(
  "file:///var/lib/neo4j/import/hb_bank.ttl",
  "Turtle"
) YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN terminationStatus, triplesLoaded, triplesParsed, extraInfo;

CALL n10s.rdf.import.fetch(
  "file:///var/lib/neo4j/import/hb_bank_data.ttl",
  "Turtle"
) YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN terminationStatus, triplesLoaded, triplesParsed, extraInfo;

