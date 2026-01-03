# Bank KG (RDF Turtle -> Neo4j n10s)

- **dbt** builds marts
- **Python** exports marts -> `hb_bank_data.ttl`
- **Neo4j (n10s)** imports ontology + data TTL
- **Airflow** validates graph exists and writes `ready_to_visualize.cypher`

## Start Neo4j

```bash
docker compose -f kg/neo4j/docker-compose.yml up -d
```

Neo4j Browser:
- http://127.0.0.1:7474/browser/
- user: `neo4j`
- password: `xxxxxxxx`

## Export TTL data + copy ontology into import/

```bash
python kg/export/export_bank_kg_data_ttl.py --data-dir data --out kg/neo4j/import/hb_bank_data.ttl
cp kg/ontology/hb_bank.ttl kg/neo4j/import/hb_bank.ttl
```

## Import (manual, in Neo4j Browser)

```cypher
// constraints
CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
FOR (r:Resource) REQUIRE r.uri IS UNIQUE;

// init n10s
CALL n10s.graphconfig.init({ handleVocabUris:'SHORTEN', typesToLabels:true });

// ontology
CALL n10s.rdf.import.fetch("file:///var/lib/neo4j/import/hb_bank.ttl","Turtle")
YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN terminationStatus, triplesLoaded, triplesParsed, extraInfo;

// data
CALL n10s.rdf.import.fetch("file:///var/lib/neo4j/import/hb_bank_data.ttl","Turtle")
YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo
RETURN terminationStatus, triplesLoaded, triplesParsed, extraInfo;
```

## Validate

```cypher
MATCH (n) RETURN count(n) AS nodes;
MATCH ()-[r]->() RETURN count(r) AS rels;
MATCH ()-[r]->() RETURN DISTINCT type(r) AS relType ORDER BY relType;
```

## Visualize

Graph view appears only when your query returns graph entities (not counts):

```cypher
MATCH p=(a)-[r]->(b)
RETURN p
LIMIT 50;
```

## Airflow

`dags/bank_kg_rdf_dag.py` into Airflow and run the DAG `bank_kg_rdf_load`.
