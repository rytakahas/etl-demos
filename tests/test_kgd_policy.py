import networkx as nx
from bankkg.kgd_networkx.dq import graph_metrics
from bankkg.kgd_networkx.dq_policy import evaluate_policy

def test_policy_violation():
    G = nx.MultiDiGraph()
    G.add_node("Customer:UNKNOWN", __label="Customer", __id="UNKNOWN")
    G.add_node("Contract:1", __label="Contract", __id="1")
    G.add_edge("Contract:1", "Customer:UNKNOWN", key="HAS_CUSTOMER", __type="HAS_CUSTOMER")

    m = graph_metrics(G)
    ok, violations = evaluate_policy(m, {"thresholds": {"orphan_edges_pct_max": 0.0}})
    assert ok is False
    assert len(violations) >= 1
