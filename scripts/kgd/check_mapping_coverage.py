#!/usr/bin/env python3
"""Check that key KG terms exist in mapping table CSV.

Usage:
  python scripts/kgd/check_mapping_coverage.py docs/kgd/03_mapping_table.csv
"""
import csv
import sys

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "docs/kgd/03_mapping_table.csv"
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    required = {"hb:Customer","hb:Contract","hb:DefaultEvent","hb:hasCustomer","hb:forContract"}
    present = {r["kg_term"] for r in rows}
    missing = sorted(required - present)
    if missing:
        print("MISSING KG TERMS:", missing)
        raise SystemExit(1)
    print("OK: required KG terms present")

if __name__ == "__main__":
    main()
