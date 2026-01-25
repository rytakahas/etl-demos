from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
import networkx as nx

def export_nodes_edges_csv(G: nx.MultiDiGraph, out_dir: str) -> Dict[str,str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    nodes: List[Dict[str, Any]] = []
    for nid, data in G.nodes(data=True):
        row = {"_id": nid, "_label": data.get("__label","Unknown")}
        for k,v in data.items():
            if k.startswith("__"):
                continue
            row[k] = v
        nodes.append(row)

    edges: List[Dict[str, Any]] = []
    for u,v,key,data in G.edges(keys=True, data=True):
        row = {"_type": data.get("__type", key), "_from": u, "_to": v}
        for k,val in data.items():
            if k.startswith("__"):
                continue
            row[k] = val
        edges.append(row)

    nodes_path = out/"nodes.csv"
    edges_path = out/"edges.csv"
    pd.DataFrame(nodes).to_csv(nodes_path, index=False)
    pd.DataFrame(edges).to_csv(edges_path, index=False)
    return {"nodes_csv": str(nodes_path), "edges_csv": str(edges_path)}
