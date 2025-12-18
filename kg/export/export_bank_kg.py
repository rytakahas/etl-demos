from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from dateutil.parser import parse as dtparse
from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef, XSD


HB = Namespace("https://example.org/honda-bank/kg#")
RES = Namespace("https://example.org/honda-bank/resource/")  # instance URIs


def _safe_date(v: object) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
        return None
    try:
        return dtparse(str(v)).date().isoformat()
    except Exception:
        return None


def _safe_decimal(v: object) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def _safe_int(v: object) -> Optional[int]:
    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def uri(kind: str, key: object) -> URIRef:
    return RES[f"{kind}/{str(key)}"]


def add_label(g: Graph, subj: URIRef, label: Optional[str]) -> None:
    if label and str(label).strip():
        g.add((subj, RDFS.label, Literal(str(label).strip())))


def main() -> None:
    ap = argparse.ArgumentParser(description="Export Honda-bank-style marts (CSV) to RDF Turtle.")
    ap.add_argument("--contracts", required=True, help="CSV like f_contract_retail (or gold contracts mart).")
    ap.add_argument("--customers", required=False, help="CSV like dim_customer.")
    ap.add_argument("--dealers", required=False, help="CSV like dim_dealer.")
    ap.add_argument("--vehicles", required=False, help="CSV like dim_vehicle.")
    ap.add_argument("--defaults", required=False, help="CSV like f_default_event.")
    ap.add_argument("--out", required=True, help="Output TTL path, e.g. data/kg/hb.ttl")
    args = ap.parse_args()

    g = Graph()
    g.bind("hb", HB)
    g.bind("res", RES)

    # ---- Load tables (CSV) ----
    contracts = pd.read_csv(args.contracts)
    customers = pd.read_csv(args.customers) if args.customers else None
    dealers = pd.read_csv(args.dealers) if args.dealers else None
    vehicles = pd.read_csv(args.vehicles) if args.vehicles else None
    defaults = pd.read_csv(args.defaults) if args.defaults else None

    # ---- Dimension nodes ----
    if customers is not None:
        for _, r in customers.iterrows():
            ck = r.get("customer_key") or r.get("customer_id") or r.get("id")
            if ck is None:
                continue
            u = uri("customer", ck)
            g.add((u, RDF.type, HB.Customer))
            g.add((u, HB.customerKey, Literal(str(ck))))
            add_label(g, u, r.get("customer_name") or r.get("name"))

    if dealers is not None:
        for _, r in dealers.iterrows():
            dk = r.get("dealer_key") or r.get("dealer_id") or r.get("id")
            if dk is None:
                continue
            u = uri("dealer", dk)
            g.add((u, RDF.type, HB.Dealer))
            g.add((u, HB.dealerKey, Literal(str(dk))))
            add_label(g, u, r.get("dealer_name") or r.get("name"))

    if vehicles is not None:
        for _, r in vehicles.iterrows():
            vk = r.get("vehicle_key") or r.get("vehicle_id") or r.get("id")
            if vk is None:
                continue
            u = uri("vehicle", vk)
            g.add((u, RDF.type, HB.Vehicle))
            g.add((u, HB.vehicleKey, Literal(str(vk))))
            add_label(g, u, r.get("model") or r.get("vehicle_model"))
            ft = r.get("fuel_type")
            if ft is not None and str(ft).strip():
                g.add((u, HB.fuelType, Literal(str(ft).strip())))
            vm = r.get("model") or r.get("vehicle_model")
            if vm is not None and str(vm).strip():
                g.add((u, HB.vehicleModel, Literal(str(vm).strip())))

    # ---- Contract nodes ----
    for _, r in contracts.iterrows():
        contract_key = r.get("contract_key") or r.get("contract_id") or r.get("id")
        if contract_key is None:
            continue

        cu = uri("contract", contract_key)
        g.add((cu, RDF.type, HB.Contract))
        g.add((cu, HB.contractKey, Literal(str(contract_key))))

        # links
        customer_key = r.get("customer_key")
        if customer_key is not None:
            g.add((cu, HB.hasCustomer, uri("customer", customer_key)))

        dealer_key = r.get("dealer_key")
        if dealer_key is not None:
            g.add((cu, HB.hasDealer, uri("dealer", dealer_key)))

        vehicle_key = r.get("vehicle_key")
        if vehicle_key is not None:
            g.add((cu, HB.hasVehicle, uri("vehicle", vehicle_key)))

        country_key = r.get("country_entity_key") or r.get("country_key") or r.get("country")
        if country_key is not None:
            co = uri("country", country_key)
            g.add((co, RDF.type, HB.Country))
            g.add((co, HB.countryKey, Literal(str(country_key))))
            g.add((cu, HB.inCountry, co))

        # measures
        amt = _safe_decimal(r.get("approved_amount"))
        if amt is not None:
            g.add((cu, HB.approvedAmount, Literal(amt, datatype=XSD.decimal)))

        tm = _safe_int(r.get("term_months"))
        if tm is not None:
            g.add((cu, HB.termMonths, Literal(tm, datatype=XSD.integer)))

        ir = _safe_decimal(r.get("interest_rate"))
        if ir is not None:
            g.add((cu, HB.interestRate, Literal(ir, datatype=XSD.decimal)))

        fr = _safe_decimal(r.get("funding_rate"))
        if fr is not None:
            g.add((cu, HB.fundingRate, Literal(fr, datatype=XSD.decimal)))

        od = _safe_date(r.get("origination_date") or r.get("orig_date") or r.get("origination_dt"))
        if od is not None:
            g.add((cu, HB.originationDate, Literal(od, datatype=XSD.date)))

    # ---- Default events ----
    if defaults is not None:
        for _, r in defaults.iterrows():
            dk = r.get("default_event_key") or r.get("default_id") or r.get("id")
            contract_key = r.get("contract_key")
            if dk is None or contract_key is None:
                continue
            du = uri("default", dk)
            g.add((du, RDF.type, HB.DefaultEvent))
            g.add((du, HB.forContract, uri("contract", contract_key)))

            da = _safe_decimal(r.get("default_amount"))
            ra = _safe_decimal(r.get("recovery_amount"))
            if da is not None:
                g.add((du, HB.defaultAmount, Literal(da, datatype=XSD.decimal)))
            if ra is not None:
                g.add((du, HB.recoveryAmount, Literal(ra, datatype=XSD.decimal)))

            dt = _safe_date(r.get("event_date") or r.get("default_date"))
            if dt is not None:
                g.add((du, HB.eventDate, Literal(dt, datatype=XSD.date)))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(g.serialize(format="turtle"), encoding="utf-8")
    print(f"✅ wrote {out} ({len(g):,} triples)")


if __name__ == "__main__":
    main()
