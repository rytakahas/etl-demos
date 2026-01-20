#!/usr/bin/env python3
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD

HB = Namespace("https://example.org/honda-bank/kg#")

IN_PATH  = "kg/neo4j/import/hb_bank_data.ttl"
OUT_PATH = "kg/neo4j/import/hb_bank_data.ttl"  # in-place


def main():
    g = Graph().parse(IN_PATH, format="turtle")

    unknown_cust = HB["Customer/UNKNOWN"]
    g.add((unknown_cust, RDF.type, HB.Customer))
    g.add((unknown_cust, HB.customerKey, Literal("UNKNOWN", datatype=XSD.string)))

    q = '''
    SELECT ?c WHERE {
      ?c a hb:Contract .
      FILTER NOT EXISTS { ?c hb:hasCustomer ?x }
    }
    '''
    missing = list(g.query(q, initNs={"hb": HB}))
    for (c,) in missing:
        g.add((c, HB.hasCustomer, unknown_cust))

    g.serialize(destination=OUT_PATH, format="turtle")
    print(f"Patched {len(missing)} contracts missing hb:hasCustomer")


if __name__ == "__main__":
    main()
