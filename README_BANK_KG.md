# Honda Bank KG layer (drop-in for etl-demos)

This folder adds an **ontology + RDF export + triplestore loading** layer on top of your existing ETL/DWH marts.

## 1) Generate RDF from marts (CSV)

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

## 2) Validate ontology (optional but good for CI)

```bash
bash kg/scripts/robot.sh
```

## 3A) Load into Fuseki (SPARQL)

```bash
bash kg/scripts/fuseki.sh
# then upload TTL:
curl -X POST -H 'Content-Type: text/turtle' --data-binary @data/kg/hb_bank.ttl \
  http://localhost:3030/pekg/data
```

## 3B) Load into RDF4J (SPARQL + SHACL)

```bash
bash kg/scripts/rdf4j.sh
# use the Workbench UI to create repo + upload data/kg/hb_bank.ttl
# and (optionally) apply SHACL from kg/shacl/bank_shapes.ttl
```

## Example SPARQL queries

### Dealer ranking by estimated lifetime margin (computed in query)
```sparql
PREFIX hb: <https://example.org/honda-bank/kg#>
PREFIX res: <https://example.org/honda-bank/resource/>

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

### Find contracts with defaults
```sparql
PREFIX hb: <https://example.org/honda-bank/kg#>

SELECT ?contract ?default ?loss
WHERE {
  ?default a hb:DefaultEvent ;
           hb:forContract ?contract ;
           hb:defaultAmount ?da ;
           hb:recoveryAmount ?ra .
  BIND((?da-?ra) AS ?loss)
}
ORDER BY DESC(?loss)
LIMIT 20
```

---

## How this fits your interview story

- **ETL/DWH**: dbt builds clean dims/facts (business truth).
- **KG semantic layer**: an ontology defines meaning, IDs, relationships.
- **RDF export**: deterministic URI patterns → graph facts.
- **Triplestore**: SPARQL endpoint for analytics + integration + governance.
