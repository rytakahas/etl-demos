from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
from rdflib.term import Node

HB = Namespace("https://example.org/honda-bank/kg#")


def norm_id(value: Union[str, int, float, None]) -> Optional[str]:
    """Normalize IDs coming from CSV (handles NaN, floats like 1.0, empty strings)."""
    if value is None:
        return None
    # pandas / numpy NaN handling
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        # Convert 1.0 -> "1"
        if value.is_integer():
            return str(int(value))
        return str(value)

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def uri(cls: str, key: Union[str, int, float]) -> Node:
    """Return a URIRef (rdflib term), not a string."""
    k = norm_id(key)
    if k is None:
        raise ValueError(f"Invalid key for URI: cls={cls}, key={key!r}")
    return HB[f"{cls}/{k}"]  # Namespace[...] returns URIRef


def add_literal(g: Graph, subj: Node, pred: Node, value, datatype: Optional[Node] = None) -> None:
    """Add a literal if value is not null/NaN."""
    if value is None:
        return
    try:
        if pd.isna(value):
            return
    except Exception:
        pass

    if datatype is None:
        g.add((subj, pred, Literal(value)))
    else:
        g.add((subj, pred, Literal(value, datatype=datatype)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data", help="Folder containing CSV marts (demo)")
    ap.add_argument("--out", default="kg/neo4j/import/hb_bank_data.ttl")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    g = Graph()
    g.bind("hb", HB)

    dim_customer = pd.read_csv(data_dir / "dim_customer.csv")
    dim_dealer = pd.read_csv(data_dir / "dim_dealer.csv")
    dim_country = pd.read_csv(data_dir / "dim_country_entity.csv")
    f_contract = pd.read_csv(data_dir / "f_contract_retail.csv")

    # Customers
    for _, r in dim_customer.iterrows():
        cust_id = norm_id(r.get("customer_key") or r.get("customer_id") or r.get("id"))
        if cust_id is None:
            continue

        s = uri("Customer", cust_id)
        g.add((s, RDF.type, HB.Customer))
        add_literal(g, s, HB.customerKey, cust_id)
        add_literal(g, s, HB.gender, r.get("gender"))
        add_literal(g, s, HB.age, r.get("age"), XSD.integer)
        add_literal(g, s, HB.countryCode, r.get("country_code"))

    # Dealers
    for _, r in dim_dealer.iterrows():
        dealer_id = norm_id(r.get("dealer_key") or r.get("dealer_id") or r.get("id"))
        if dealer_id is None:
            continue

        s = uri("Dealer", dealer_id)
        g.add((s, RDF.type, HB.Dealer))
        add_literal(g, s, HB.dealerKey, dealer_id)
        add_literal(g, s, HB.dealerName, r.get("dealer_name"))

    # Countries
    for _, r in dim_country.iterrows():
        c_id = norm_id(r.get("country_entity_key") or r.get("country_code") or r.get("id"))
        if c_id is None:
            continue

        s = uri("Country", c_id)
        g.add((s, RDF.type, HB.Country))
        add_literal(g, s, HB.countryEntityKey, c_id)
        add_literal(g, s, HB.countryName, r.get("country_name"))
        add_literal(g, s, HB.countryCode, r.get("country_code"))

    # Contracts + edges
    for _, r in f_contract.iterrows():
        contract_id = norm_id(r.get("contract_key") or r.get("contract_id") or r.get("id"))
        if contract_id is None:
            continue

        s_contract = uri("Contract", contract_id)
        g.add((s_contract, RDF.type, HB.Contract))
        add_literal(g, s_contract, HB.contractKey, contract_id)
        add_literal(g, s_contract, HB.approvedAmount, r.get("approved_amount"), XSD.decimal)
        add_literal(g, s_contract, HB.interestRate, r.get("interest_rate"), XSD.decimal)
        add_literal(g, s_contract, HB.termMonths, r.get("term_months"), XSD.integer)

        cust_key = norm_id(r.get("customer_key"))
        if cust_key is not None:
            s_cust = uri("Customer", cust_key)
            g.add((s_cust, HB.hasContract, s_contract))

        dealer_key = norm_id(r.get("dealer_key"))
        if dealer_key is not None:
            s_dealer = uri("Dealer", dealer_key)
            g.add((s_contract, HB.soldByDealer, s_dealer))

        country_key = norm_id(r.get("country_entity_key"))
        if country_key is not None:
            s_country = uri("Country", country_key)
            g.add((s_contract, HB.inCountry, s_country))

    g.serialize(destination=str(out_path), format="turtle")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()

