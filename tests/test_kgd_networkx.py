import pandas as pd
from bankkg.kgd_networkx.spec import KGDSpec
from bankkg.kgd_networkx.build import build_graph
from bankkg.kgd_networkx.dq import run_all_checks

def test_build_graph_smoke():
    spec = KGDSpec(**{
        "version": 1,
        "gold_sources": {"customers":"dim_customer.csv","contracts":"f_contract_retail.csv"},
        "nodes": [
            {"label":"Customer","table":"customers","id_column":"customer_key","properties":["segment"]},
            {"label":"Contract","table":"contracts","id_column":"contract_key","properties":["term_months"]},
        ],
        "edges": [
            {"type":"HAS_CUSTOMER","table":"contracts","from_label":"Contract","from_id_column":"contract_key","to_label":"Customer","to_id_column":"customer_key","properties":[]}
        ],
    })
    tables = {
        "customers": pd.DataFrame([{"customer_key":"1","segment":"A"}]),
        "contracts": pd.DataFrame([{"contract_key":"10","customer_key":"1","term_months":24}]),
    }
    G = build_graph(tables, spec)
    assert G.number_of_nodes() >= 2
    assert G.number_of_edges() >= 1
    rep = run_all_checks(G)
    assert "stats" in rep
