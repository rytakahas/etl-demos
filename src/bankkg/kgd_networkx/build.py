from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any
import pandas as pd
import networkx as nx

@dataclass(frozen=True)
class NodeKey:
    label: str
    id: str
    def as_str(self) -> str:
        return f"{self.label}:{self.id}"

def build_graph(tables: Dict[str, pd.DataFrame], spec) -> nx.MultiDiGraph:
    G = nx.MultiDiGraph()

    # Nodes
    for n in spec.nodes:
        df = tables.get(n.table, pd.DataFrame())
        if df.empty or n.id_column not in df.columns:
            continue
        for _, r in df.iterrows():
            node_id = str(r[n.id_column])
            key = NodeKey(n.label, node_id).as_str()
            props: Dict[str, Any] = {"__label": n.label, "__id": node_id}
            for p in n.properties:
                if p in df.columns and pd.notna(r[p]):
                    props[p] = r[p]
            G.add_node(key, **props)

    # Edges
    for e in spec.edges:
        df = tables.get(e.table, pd.DataFrame())
        if df.empty:
            continue
        if e.from_id_column not in df.columns or e.to_id_column not in df.columns:
            continue
        for _, r in df.iterrows():
            u = NodeKey(e.from_label, str(r[e.from_id_column])).as_str()
            v = NodeKey(e.to_label, str(r[e.to_id_column])).as_str()
            if not G.has_node(u):
                G.add_node(u, __label=e.from_label, __id=str(r[e.from_id_column]))
            if not G.has_node(v):
                G.add_node(v, __label=e.to_label, __id=str(r[e.to_id_column]))
            props: Dict[str, Any] = {"__type": e.type}
            for p in e.properties:
                if p in df.columns and pd.notna(r[p]):
                    props[p] = r[p]
            G.add_edge(u, v, key=e.type, **props)

    return G
