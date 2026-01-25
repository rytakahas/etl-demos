from __future__ import annotations
from typing import Dict, Any, List, Tuple
import networkx as nx

def graph_stats(G: nx.MultiDiGraph) -> Dict[str, Any]:
    labels: Dict[str,int] = {}
    for _, data in G.nodes(data=True):
        lbl = data.get("__label","Unknown")
        labels[lbl] = labels.get(lbl,0)+1

    rels: Dict[str,int] = {}
    for _,_, data in G.edges(data=True):
        t = data.get("__type","UNKNOWN_REL")
        rels[t] = rels.get(t,0)+1

    return {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "node_labels": sorted(labels.items(), key=lambda x:x[1], reverse=True)[:50],
        "edge_types": sorted(rels.items(), key=lambda x:x[1], reverse=True)[:50],
    }

def orphan_edges(G: nx.MultiDiGraph) -> int:
    # placeholder nodes = only __label/__id
    def is_placeholder(n: str) -> bool:
        return set(G.nodes[n].keys()).issubset({"__label","__id"})
    cnt = 0
    for u,v,_k in G.edges(keys=True):
        if is_placeholder(u) or is_placeholder(v):
            cnt += 1
    return cnt

def supernodes(G: nx.MultiDiGraph, threshold: int = 5000) -> List[Tuple[str,int]]:
    out = []
    for n in G.nodes:
        deg = G.degree(n)
        if deg >= threshold:
            out.append((n, deg))
    return sorted(out, key=lambda x:x[1], reverse=True)

def run_all_checks(G: nx.MultiDiGraph) -> Dict[str, Any]:
    return {
        "stats": graph_stats(G),
        "orphan_edges": orphan_edges(G),
        "supernodes": supernodes(G, threshold=5000),
    }
