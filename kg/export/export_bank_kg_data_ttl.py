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

DEFAULT_BASE_IRI = "https://example.org/honda-bank/kg#"
PROV = Namespace("http://www.w3.org/ns/prov#")


def norm_id(value: Union[str, int, float, None]) -> Optional[str]:
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
        return str(int(value)) if value.is_integer() else str(value)
    s = str(value).strip()
    return None if not s or s.lower() == "nan" else s


def add_literal(g: Graph, subj: Node, pred: Node, value, datatype: Optional[Node] = None) -> None:
    if value is None:
        return
    try:
        if pd.isna(value):
            return
    except Exception:
        pass
    g.add((subj, pred, Literal(value, datatype=datatype) if datatype else Literal(value)))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return os.getenv("GIT_SHA", "unknown")


def new_run_id() -> str:
    return uuid4().hex[:12]


def date_from_yyyymmdd(value) -> Optional[str]:
    v = norm_id(value)
    if v is None:
        return None
    v = v.replace("-", "").replace("/", "").replace(".", "")
    if len(v) != 8 or not v.isdigit():
        return None
    return f"{v[0:4]}-{v[4:6]}-{v[6:8]}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--out", default="kg/ontology/hb_bank_data.ttl")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--base-iri", default=os.getenv("KG_BASE_IRI", DEFAULT_BASE_IRI))
    ap.add_argument("--add-unknown-customer", action="store_true")
    args = ap.parse_args()

    HB = Namespace(args.base_iri)

    def uri(cls: str, key: Union[str, int, float]) -> Node:
        k = norm_id(key)
        if k is None:
            raise ValueError(f"Invalid key for URI: cls={cls}, key={key!r}")
        return HB[f"{cls}/{k}"]

    data_dir = Path(args.data_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    g = Graph()
    g.bind("hb", HB)
    g.bind("prov", PROV)

    run_id = args.run_id or os.getenv("KG_RUN_ID") or new_run_id()
    run = HB[f"DatasetRun/{run_id}"]

    g.add((run, RDF.type, PROV.Activity))
    g.add((run, RDF.type, HB.DatasetRun))
    g.add((run, PROV.startedAtTime, Literal(utc_now_iso(), datatype=XSD.dateTime)))
    g.add((run, HB.gitCommit, Literal(git_sha(), datatype=XSD.string)))

    # Load marts (Gold-like CSVs)
    dim_customer = pd.read_csv(data_dir / "dim_customer.csv")
    dim_dealer = pd.read_csv(data_dir / "dim_dealer.csv")
    dim_country = pd.read_csv(data_dir / "dim_country_entity.csv")
    dim_vehicle = pd.read_csv(data_dir / "dim_vehicle.csv")
    f_contract = pd.read_csv(data_dir / "f_contract_retail.csv")
    f_default = pd.read_csv(data_dir / "f_default_event.csv")
    f_payment = pd.read_csv(data_dir / "f_payment.csv")

    # Customers
    for _, r in dim_customer.iterrows():
        cust_id = norm_id(r.get("customer_key") or r.get("customer_id") or r.get("id"))
        if cust_id is None:
            continue
        s = uri("Customer", cust_id)
        g.add((s, RDF.type, HB.Customer))
        g.add((s, PROV.wasGeneratedBy, run))
        add_literal(g, s, HB.customerKey, cust_id, XSD.string)
        add_literal(g, s, HB.segment, r.get("segment"), XSD.string)

    # Dealers
    for _, r in dim_dealer.iterrows():
        dealer_id = norm_id(r.get("dealer_key") or r.get("dealer_id") or r.get("id"))
        if dealer_id is None:
            continue
        s = uri("Dealer", dealer_id)
        g.add((s, RDF.type, HB.Dealer))
        g.add((s, PROV.wasGeneratedBy, run))
        add_literal(g, s, HB.dealerKey, dealer_id, XSD.string)

    # Countries
    for _, r in dim_country.iterrows():
        c_id = norm_id(r.get("country_entity_key") or r.get("country_code") or r.get("id"))
        if c_id is None:
            continue
        s = uri("Country", c_id)
        g.add((s, RDF.type, HB.Country))
        g.add((s, PROV.wasGeneratedBy, run))
        add_literal(g, s, HB.countryKey, c_id, XSD.string)
        add_literal(g, s, HB.countryCode, r.get("country_code"), XSD.string)
        add_literal(g, s, HB.countryName, r.get("country_name"), XSD.string)

    # Vehicles (emit both vehicleModel + canonical model)
    for _, r in dim_vehicle.iterrows():
        v_id = norm_id(r.get("vehicle_key") or r.get("model_code") or r.get("id"))
        if v_id is None:
            continue
        s = uri("Vehicle", v_id)
        g.add((s, RDF.type, HB.Vehicle))
        g.add((s, PROV.wasGeneratedBy, run))
        add_literal(g, s, HB.vehicleKey, v_id, XSD.string)

        model_name = r.get("model_name")
        add_literal(g, s, HB.vehicleModel, model_name, XSD.string)  # existing
        add_literal(g, s, HB.model, model_name, XSD.string)         # canonical

        add_literal(g, s, HB.fuelType, r.get("fuel_type"), XSD.string)
        add_literal(g, s, HB.vin, r.get("vin") or r.get("vehicle_vin"), XSD.string)

    # Unknown customer (optional)
    unknown_cust = HB["Customer/UNKNOWN"]
    if args.add_unknown_customer:
        g.add((unknown_cust, RDF.type, HB.Customer))
        g.add((unknown_cust, PROV.wasGeneratedBy, run))
        add_literal(g, unknown_cust, HB.customerKey, "UNKNOWN", XSD.string)

    # Contracts
    for _, r in f_contract.iterrows():
        contract_id = norm_id(r.get("contract_key") or r.get("contract_id") or r.get("id"))
        if contract_id is None:
            continue
        s_contract = uri("Contract", contract_id)
        g.add((s_contract, RDF.type, HB.Contract))
        g.add((s_contract, PROV.wasGeneratedBy, run))

        add_literal(g, s_contract, HB.contractKey, contract_id, XSD.string)
        add_literal(g, s_contract, HB.approvedAmount, r.get("approved_amount"), XSD.decimal)
        add_literal(g, s_contract, HB.interestRate, r.get("interest_rate"), XSD.decimal)
        add_literal(g, s_contract, HB.termMonths, r.get("term_months"), XSD.integer)

        cust_key = norm_id(r.get("customer_key"))
        if cust_key is not None:
            g.add((s_contract, HB.hasCustomer, uri("Customer", cust_key)))
        elif args.add_unknown_customer:
            g.add((s_contract, HB.hasCustomer, unknown_cust))

        dealer_key = norm_id(r.get("dealer_key"))
        if dealer_key is not None:
            g.add((s_contract, HB.hasDealer, uri("Dealer", dealer_key)))

        country_key = norm_id(r.get("country_entity_key"))
        if country_key is not None:
            g.add((s_contract, HB.inCountry, uri("Country", country_key)))

        veh_key = norm_id(r.get("vehicle_key"))
        if veh_key is not None:
            g.add((s_contract, HB.hasVehicle, uri("Vehicle", veh_key)))

    # DefaultEvent (emit canonical eventType)
    for _, r in f_default.iterrows():
        event_key = norm_id(r.get("default_event_key") or r.get("event_id") or r.get("id"))
        contract_id = norm_id(r.get("contract_key") or r.get("contract_id"))
        if contract_id is None:
            continue
        event_date = date_from_yyyymmdd(r.get("default_date_key"))
        if event_date is None:
            continue
        if event_key is None:
            event_key = f"{contract_id}-{event_date}"

        s_event = uri("DefaultEvent", event_key)
        g.add((s_event, RDF.type, HB.DefaultEvent))
        g.add((s_event, PROV.wasGeneratedBy, run))

        g.add((s_event, HB.forContract, uri("Contract", contract_id)))
        add_literal(g, s_event, HB.eventDate, event_date, XSD.date)
        add_literal(g, s_event, HB.defaultAmount, r.get("default_amount"), XSD.decimal)
        add_literal(g, s_event, HB.recoveryAmount, r.get("recovery_amount"), XSD.decimal)

        add_literal(g, s_event, HB.eventType, r.get("event_type") or "Default", XSD.string)

    # Payment (emit canonical amountPaid + daysPastDue)
    for _, r in f_payment.iterrows():
        pay_key = norm_id(r.get("payment_key") or r.get("id"))
        contract_id = norm_id(r.get("contract_key"))
        if pay_key is None or contract_id is None:
            continue
        pay_date = date_from_yyyymmdd(r.get("payment_date_key"))
        if pay_date is None:
            continue

        s_pay = uri("Payment", pay_key)
        g.add((s_pay, RDF.type, HB.Payment))
        g.add((s_pay, PROV.wasGeneratedBy, run))

        add_literal(g, s_pay, HB.paymentKey, pay_key, XSD.string)
        g.add((s_pay, HB.paymentForContract, uri("Contract", contract_id)))
        add_literal(g, s_pay, HB.paymentDate, pay_date, XSD.date)

        scheduled = r.get("scheduled_amount")
        paid_amt = r.get("paid_amount")

        add_literal(g, s_pay, HB.scheduledAmount, scheduled, XSD.decimal)
        add_literal(g, s_pay, HB.paidAmount, paid_amt, XSD.decimal)     # existing
        add_literal(g, s_pay, HB.amountPaid, paid_amt, XSD.decimal)     # canonical

        dpd = r.get("dpd_at_payment")
        try:
            dpd = int(dpd) if dpd is not None and not pd.isna(dpd) else None
        except Exception:
            dpd = None
        add_literal(g, s_pay, HB.dpdAtPayment, dpd, XSD.integer)         # existing
        add_literal(g, s_pay, HB.daysPastDue, dpd, XSD.integer)          # canonical

    g.serialize(destination=str(out_path), format="turtle")
    print(f"wrote {out_path} (run_id={run_id}, base={args.base_iri})")


if __name__ == "__main__":
    main()
