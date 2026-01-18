#!/usr/bin/env python3
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD

HB = Namespace("https://example.org/honda-bank/kg#")

IN_PATH  = "/usr/local/airflow/kg/neo4j/import/hb_bank_data.ttl"
OUT_PATH = "/usr/local/airflow/kg/neo4j/import/hb_bank_data.ttl"  # in-place

def main():
    g = Graph().parse(IN_PATH, format="turtle")

    # Create an UNKNOWN customer node once
    unknown_cust = URIRef(str(HB) + "Customer/UNKNOWN")
    g.add((unknown_cust, RDF.type, HB.Customer))
    # optional: if you use customerKey identity
    g.add((unknown_cust, HB.customerKey, Literal("UNKNOWN", datatype=XSD.string)))

    # Patch contracts missing hasCustomer
    q = """
    SELECT ?c WHERE {
      ?c a hb:Contract .
      FILTER NOT EXISTS { ?c hb:hasCustomer ?x }
    }
    """
    missing = list(g.query(q, initNs={"hb": HB}))
    for (c,) in missing:
        g.add((c, HB.hasCustomer, unknown_cust))

    g.serialize(destination=OUT_PATH, format="turtle")
    print(f"Patched {len(missing)} contracts missing hb:hasCustomer")

if __name__ == "__main__":
    main()

