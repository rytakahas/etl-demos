from __future__ import annotations

import argparse
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union
from uuid import uuid4

import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
from rdflib.term import Node

# Your KG namespace (your "IRI base")
HB = Namespace("https://example.org/honda-bank/kg#")
# Standard provenance vocabulary (W3C PROV-O)
PROV = Namespace("http://www.w3.org/ns/prov#")


def norm_id(value: Union[str, int, float, None]) -> Optional[str]:
    """Normalize IDs coming from CSV (handles NaN, floats like 1.0, empty strings)."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def uri(cls: str, key: Union[str, int, float]) -> Node:
    """Return a URIRef (rdflib term) for an entity instance."""
    k = norm_id(key)
    if k is None:
        raise ValueError(f"Invalid key for URI: cls={cls}, key={key!r}")
    return HB[f"{cls}/{k}"]


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_sha() -> str:
    """Best-effort git SHA for provenance."""
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return os.getenv("GIT_SHA", "unknown")


def new_run_id() -> str:
    return uuid4().hex[:12]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data", help="Folder containing CSV marts (demo)")
    ap.add_argument("--out", default="kg/neo4j/import/hb_bank_data.ttl")
    ap.add_argument("--run-id", default=None, help="Optional run id (for provenance).")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    g = Graph()
    g.bind("hb", HB)
    g.bind("prov", PROV)

    # -------------------------
    # Provenance (Dataset Run)
    # -------------------------
    run_id = args.run_id or os.getenv("KG_RUN_ID") or new_run_id()
    run = HB[f"DatasetRun/{run_id}"]

    # run node types
    g.add((run, RDF.type, PROV.Activity))
    g.add((run, RDF.type, HB.DatasetRun))  # optional custom class under hb:

    # minimal metadata
    g.add((run, PROV.startedAtTime, Literal(utc_now_iso(), datatype=XSD.dateTime)))
    g.add((run, HB.gitCommit, Literal(git_sha(), datatype=XSD.string)))

    # list which source files/models were used for this export
    source_models = [
        "dim_customer.csv",
        "dim_dealer.csv",
        "dim_country_entity.csv",
        "f_contract_retail.csv",
    ]
    for m in source_models:
        g.add((run, HB.sourceModel, Literal(m, datatype=XSD.string)))

    # -------------------------
    # Load "Gold-like" marts
    # -------------------------
    dim_customer = pd.read_csv(data_dir / "dim_customer.csv")
    dim_dealer = pd.read_csv(data_dir / "dim_dealer.csv")
    dim_country = pd.read_csv(data_dir / "dim_country_entity.csv")
    f_contract = pd.read_csv(data_dir / "f_contract_retail.csv")

    # -------------------------
    # Customers (dimension -> entity nodes)
    # -------------------------
    for _, r in dim_customer.iterrows():
        cust_id = norm_id(r.get("customer_key") or r.get("customer_id") or r.get("id"))
        if cust_id is None:
            continue

        s = uri("Customer", cust_id)
        g.add((s, RDF.type, HB.Customer))
        g.add((s, PROV.wasGeneratedBy, run))

        # keys + attributes (PII-light)
        add_literal(g, s, HB.customerKey, cust_id, XSD.string)
        add_literal(g, s, HB.gender, r.get("gender"), XSD.string)
        add_literal(g, s, HB.age, r.get("age"), XSD.integer)
        add_literal(g, s, HB.countryCode, r.get("country_code"), XSD.string)

    # -------------------------
    # Dealers (dimension -> entity nodes)
    # -------------------------
    for _, r in dim_dealer.iterrows():
        dealer_id = norm_id(r.get("dealer_key") or r.get("dealer_id") or r.get("id"))
        if dealer_id is None:
            continue

        s = uri("Dealer", dealer_id)
        g.add((s, RDF.type, HB.Dealer))
        g.add((s, PROV.wasGeneratedBy, run))

        add_literal(g, s, HB.dealerKey, dealer_id, XSD.string)
        add_literal(g, s, HB.dealerName, r.get("dealer_name"), XSD.string)

    # -------------------------
    # Countries (dimension -> entity nodes)
    # -------------------------
    for _, r in dim_country.iterrows():
        c_id = norm_id(r.get("country_entity_key") or r.get("country_code") or r.get("id"))
        if c_id is None:
            continue

        s = uri("Country", c_id)
        g.add((s, RDF.type, HB.Country))
        g.add((s, PROV.wasGeneratedBy, run))

        add_literal(g, s, HB.countryKey, c_id, XSD.string)
        add_literal(g, s, HB.countryName, r.get("country_name"), XSD.string)
        add_literal(g, s, HB.countryCode, r.get("country_code"), XSD.string)

    # -------------------------
    # Contracts (fact-ish business object -> node + edges to dims)
    # -------------------------
    for _, r in f_contract.iterrows():
        contract_id = norm_id(r.get("contract_key") or r.get("contract_id") or r.get("id"))
        if contract_id is None:
            continue

        s_contract = uri("Contract", contract_id)
        g.add((s_contract, RDF.type, HB.Contract))
        g.add((s_contract, PROV.wasGeneratedBy, run))

        # measures
        add_literal(g, s_contract, HB.contractKey, contract_id, XSD.string)
        add_literal(g, s_contract, HB.approvedAmount, r.get("approved_amount"), XSD.decimal)
        add_literal(g, s_contract, HB.interestRate, r.get("interest_rate"), XSD.decimal)
        add_literal(g, s_contract, HB.termMonths, r.get("term_months"), XSD.integer)

        # edges aligned with ontology:
        # Contract -> hasCustomer -> Customer
        cust_key = norm_id(r.get("customer_key"))
        if cust_key is not None:
            s_cust = uri("Customer", cust_key)
            g.add((s_contract, HB.hasCustomer, s_cust))

        # Contract -> hasDealer -> Dealer
        dealer_key = norm_id(r.get("dealer_key"))
        if dealer_key is not None:
            s_dealer = uri("Dealer", dealer_key)
            g.add((s_contract, HB.hasDealer, s_dealer))

        # Contract -> inCountry -> Country
        country_key = norm_id(r.get("country_entity_key"))
        if country_key is not None:
            s_country = uri("Country", country_key)
            g.add((s_contract, HB.inCountry, s_country))

    g.serialize(destination=str(out_path), format="turtle")
    print(f"wrote {out_path} (run_id={run_id}, git={git_sha()})")


if __name__ == "__main__":
    main()
