from __future__ import annotations
from typing import Dict, Any, List, Tuple
import networkx as nx


def _topk_counts(items: List[str], k: int = 20) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {}
    for x in items:
        counts[x] = counts.get(x, 0) + 1
    return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:k]


def graph_metrics(G: nx.MultiDiGraph) -> Dict[str, Any]:
    nodes = G.number_of_nodes()
    edges = G.number_of_edges()

    node_labels = [G.nodes[n].get("__label", "Unknown") for n in G.nodes]
    edge_types = [d.get("__type", "UNKNOWN_REL") for _, _, d in G.edges(data=True)]

    def is_placeholder(n: str) -> bool:
        d = G.nodes[n]
        keys = set(d.keys())
        placeholder = keys.issubset({"__label", "__id"})
        unknown = str(d.get("__id", "")).upper() == "UNKNOWN"
        return placeholder or unknown

    placeholder_nodes = sum(1 for n in G.nodes if is_placeholder(n))
    unknown_node_pct = (placeholder_nodes / nodes) if nodes else 0.0

    orphan_edges = 0
    for u, v, _k in G.edges(keys=True):
        if is_placeholder(u) or is_placeholder(v):
            orphan_edges += 1
    orphan_edges_pct = (orphan_edges / edges) if edges else 0.0

    UG = nx.Graph()
    UG.add_nodes_from(G.nodes)
    UG.add_edges_from((u, v) for u, v in G.edges())
    components = nx.number_connected_components(UG) if nodes else 0
    largest_component = max((len(c) for c in nx.connected_components(UG)), default=0)
    largest_component_share = (largest_component / nodes) if nodes else 0.0

    degrees = dict(G.degree())
    max_degree = max(degrees.values()) if degrees else 0

    return {
        "nodes": nodes,
        "edges": edges,
        "placeholder_nodes": placeholder_nodes,
        "unknown_node_pct": unknown_node_pct,
        "orphan_edges": orphan_edges,
        "orphan_edges_pct": orphan_edges_pct,
        "components": components,
        "largest_component_share": largest_component_share,
        "max_degree": max_degree,
        "node_labels_top": _topk_counts(node_labels, k=50),
        "edge_types_top": _topk_counts(edge_types, k=50),
    }


def supernodes(G: nx.MultiDiGraph, threshold: int = 5000) -> List[Tuple[str, int]]:
    out = []
    for n in G.nodes:
        deg = G.degree(n)
        if deg >= threshold:
            out.append((n, deg))
    return sorted(out, key=lambda x: x[1], reverse=True)


def run_all_checks(G: nx.MultiDiGraph, supernode_threshold: int = 5000) -> Dict[str, Any]:
    m = graph_metrics(G)
    return {
        "metrics": m,
        "supernodes": supernodes(G, threshold=supernode_threshold),
    }
