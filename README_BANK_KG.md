# Bank KG Layer (Graph DB / RDF Semantic Layer)

This folder adds an **ontology + RDF export + graph loading** layer on top of your existing **ETL + star-schema marts**.

## Intent and Purpose

- **DWH / star schema (dbt dims + facts)** remains the *system of record* for BI metrics, KPI dashboards, and aggregates.
- **Graph / KG layer** is a *semantic + relationship-first layer* for:
  - multi-hop questions ("who is connected to what?")
  - integration across domains / sources
  - governance (IDs, meaning, lineage)
  - "network-style" analytics (paths, communities, centrality, risk propagation)

---

## Why a Graph / Knowledge Graph on Top of a Star Schema?

### What the Star Schema Is Best At

A Kimball-style star schema organizes analytics around a business process with:
- a **fact table** (events/measures)
- multiple **dimension tables** (context/attributes)

This is ideal for:
- slicing & dicing KPIs
- predictable joins
- OLAP-style reporting

### Where Graphs Add Value

Graphs shine when your questions are naturally expressed as **relationships and traversals**, not just aggregations:

- **Relationship discovery**: "Which customers are connected to the same dealer network and show similar default patterns?"
- **Multi-hop reasoning**: "Customers → contracts → dealers → region → macro indicators → defaults"
- **Entity resolution & integration**: linking the same "Dealer" or "Customer" across multiple systems using shared identifiers and ontology-defined meaning (RDF/OWL)
- **Governance + semantics**: a shared vocabulary (ontology) makes data interoperable and explainable

Neo4j's modeling guidance is explicit: a graph model should be built from **use-cases / questions** first, then represent entities + relationships to answer them efficiently.

---

## Same vs Different: Star Schema Modeling vs Graph Modeling

### What Is the Same

Both approaches start the same way:
1. **Define the business questions** (use cases)
2. **Define grain** (what is a "row" / what is an "event"?)
3. Define canonical **IDs** and **conformed dimensions** (consistent Customer/Dealer/Vehicle identity)

### What Is Different

**Star Schema**
- Optimized for SQL joins + aggregates
- Facts/dims are table-first structures
- Relationships are "implicit" via FK/PK joins

**Graph**
- Optimized for **traversals** (pattern matching)
- Relationships are **first-class** and stored explicitly
- You often model "verbs" as relationships and "things" as nodes

A good mental mapping:
- **Dimensions** → usually become **nodes** (Customer, Dealer, Vehicle, Country, Date…)
- **Facts** → become either:
  - **event nodes** (Contract, Payment, DefaultEvent), or
  - **relationships with properties** (if the "fact" is best understood as a link between two entities and you don't need to attach many other links to it)

---

## Thumb Rules: What Should Be Nodes vs Edges?

Neo4j property graph basics:
- Nodes represent entities ("things")
- Relationships represent connections ("verbs") and may also have properties

### Rule 1 — If It Has Its Own Identity, Make It a Node

Make it a **node** if:
- it has a stable ID (contract_id, payment_id)
- you need to link it to many entities (customer, dealer, vehicle, country, date)
- you may query/filter it directly (e.g., contracts above X, defaults in period Y)

**Examples (bank domain):**
- `(:Customer)`
- `(:Dealer)`
- `(:Vehicle)`
- `(:Country)`
- `(:Contract)` ← usually a node (important event entity)
- `(:Payment)` / `(:DefaultEvent)` (if modeled)

### Rule 2 — If It's a Simple "Verb" Between Two Nodes, Prefer a Relationship

Use a **relationship** when:
- it's a clean connection between two entities
- you mostly traverse through it
- it doesn't need complex sub-links

**Examples:**
- `(Customer)-[:HAS_CONTRACT]->(Contract)`
- `(Contract)-[:SOLD_BY]->(Dealer)`
- `(Contract)-[:IN_COUNTRY]->(Country)`

### Rule 3 — Relationship Properties Are OK, But Don't Overdo Them

Relationships can hold properties (rates, amounts), but if you keep adding properties + extra links, it's a smell that you want an event node. (This is also why `Contract` is usually best as a node in banking analytics.)

### Rule 4 — Model for the Query You Actually Run

Graph models are **query-driven**. Neo4j explicitly recommends modeling based on the use cases you need to support.

---

## A Realistic "Bank Graph" Model for This Repo

Your star schema has a natural graph projection:

### Likely Nodes (Entities)
- `Customer`
- `Dealer`
- `Vehicle`
- `CountryEntity` / `Country`
- `Contract` (retail/wholesale)
- `DefaultEvent`
- (Optional) `Date` (if you want calendar traversals & time bucketing as nodes)

### Likely Relationships (Verbs)
- `(:Customer)-[:HAS_CONTRACT]->(:Contract)`
- `(:Contract)-[:SOLD_BY]->(:Dealer)`
- `(:Contract)-[:HAS_VEHICLE]->(:Vehicle)`
- `(:Contract)-[:IN_COUNTRY]->(:Country)`
- `(:Contract)-[:DEFAULTED_AS]->(:DefaultEvent)` (or `(:DefaultEvent)-[:FOR_CONTRACT]->(:Contract)`)

This makes common traversal questions easy:
- Dealer exposure (dealer → contracts → defaults)
- Customer journey (customer → contracts → vehicles)
- Risk propagation patterns (shared dealers, shared vehicle types, shared regions)

---

## RDF / Ontology Angle (Why RDF Here?)

RDF represents knowledge as a **graph of triples** (subject–predicate–object).

Using RDF + an ontology (OWL/RDFS) gives you:
- globally unique identifiers (IRIs)
- a shared vocabulary (classes + properties)
- interoperability with other systems (SPARQL, SHACL)

Neosemantics (n10s) can import RDF into Neo4j using `n10s.rdf.import.fetch`.

In practice:
- **Ontology (TBox)** defines meaning (Customer, Contract, soldBy, etc.)
- **Instance data (ABox)** contains facts (Customer/123 hasContract Contract/999)

---

## Performance & Modeling Tips (Practical)

### 1) Put Constraints / Indexes on IDs You Match On

For Neo4j, constraints + indexes are core for performance and data integrity.

At minimum, unique constraints on your canonical IDs (customerKey, contractKey, dealerKey).

### 2) Avoid "Supernodes" When Possible

Very high-degree nodes (e.g., one Country node connected to millions of contracts) can become hotspots.

Common patterns to mitigate:
- add intermediate nodes (e.g., `CountryMonth`)
- partition by time
- or keep some attributes as properties instead of edges

### 3) Start Queries from Selective Anchors

In Cypher, try to start from an indexed lookup (e.g., `MATCH (c:Customer {customerKey: ...}) ...`) and then expand. Use `EXPLAIN` / `PROFILE` to tune query plans.

---

## Pipeline (Technical Steps)

### 1) Generate RDF from Marts (CSV)

First export your gold tables to CSV (BigQuery → local CSV, Postgres → CSV, etc).

Then:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r kg/export/requirements-kg.txt

python kg/export/export_bank_kg.py \
  --contracts data/marts/f_contract_retail.csv \
  --customers data/marts/dim_customer.csv \
  --dealers data/marts/dim_dealer.csv \
  --vehicles data/marts/dim_vehicle.csv \
  --defaults data/marts/f_default_event.csv \
  --out data/kg/hb_bank.ttl
```

### 2) Validate Ontology (Optional but Good for CI)

```bash
bash kg/scripts/robot.sh
```

### 3A) Load into Fuseki (SPARQL)

```bash
bash kg/scripts/fuseki.sh

curl -X POST -H 'Content-Type: text/turtle' --data-binary @data/kg/hb_bank.ttl \
  http://localhost:3030/pekg/data
```

### 3B) Load into RDF4J (SPARQL + SHACL)

```bash
bash kg/scripts/rdf4j.sh
```

Use the Workbench UI to create repo + upload `hb_bank.ttl`. Optionally apply SHACL from `kg/shacl/bank_shapes.ttl`.

### 3C) Load into Neo4j (Property Graph via n10s)

Once loaded, you can inspect schema in Neo4j Browser:

```cypher
CALL db.schema.visualization();
```

---

## Example SPARQL Queries

### Dealer Ranking by Estimated Lifetime Margin (Computed in Query)

```sparql
PREFIX hb: <https://example.org/honda-bank/kg#>

SELECT ?dealer (COUNT(?c) AS ?n_contracts) (SUM(?margin) AS ?total_margin)
WHERE {
  ?c a hb:Contract ;
     hb:hasDealer ?dealer ;
     hb:approvedAmount ?amt ;
     hb:interestRate ?ir ;
     hb:fundingRate ?fr ;
     hb:termMonths ?tm .
  BIND( (?amt * ((?ir-?fr)/100.0) * (?tm/12.0)) AS ?margin )
}
GROUP BY ?dealer
ORDER BY DESC(?total_margin)
LIMIT 20
```

---

## Example Cypher Queries (Neo4j)

### Show Any Graph

```cypher
MATCH p=()-[]-()
RETURN p
LIMIT 200;
```

### Domain-Only View (Hide OWL/RDFS Meta Nodes)

```cypher
MATCH p=(a)-[r]->(b)
WHERE NONE(l IN labels(a) WHERE l STARTS WITH "owl__" OR l STARTS WITH "rdfs__")
  AND NONE(l IN labels(b) WHERE l STARTS WITH "owl__" OR l STARTS WITH "rdfs__")
RETURN p
LIMIT 200;
```

### Quick Counts

```cypher
CALL db.labels() YIELD label
CALL {
  WITH label
  MATCH (n) WHERE label IN labels(n)
  RETURN count(n) AS c
}
RETURN label, c
ORDER BY c DESC;
```

---

## Summary

This KG layer complements your star schema by providing a semantic, relationship-first view of your bank data. It enables multi-hop queries, entity resolution, and network analytics while maintaining the star schema as the authoritative source for metrics and reporting.
