from __future__ import annotations

from typing import TypedDict, Dict, Any
from pathlib import Path
import yaml
import pandas as pd

from langgraph.graph import StateGraph, END

from bankkg.kgd_networkx.spec import KGDSpec
from bankkg.kgd_networkx.build import build_graph
from bankkg.kgd_networkx.dq import run_all_checks
from bankkg.kgd_networkx.export import export_nodes_edges_csv

class KGDState(TypedDict, total=False):
    spec_path: str
    gold_dir: str
    out_dir: str
    report: Dict[str, Any]
    outputs: Dict[str, str]

def load_inputs(state: KGDState) -> KGDState:
    spec = KGDSpec(**yaml.safe_load(Path(state["spec_path"]).read_text(encoding="utf-8")))
    gold_dir = Path(state["gold_dir"])
    tables = {}
    for name, fname in spec.gold_sources.items():
        fp = gold_dir / fname
        tables[name] = pd.read_csv(fp) if fp.exists() else pd.DataFrame()
    state["_spec"] = spec
    state["_tables"] = tables
    return state

def build(state: KGDState) -> KGDState:
    state["_G"] = build_graph(state["_tables"], state["_spec"])
    return state

def dq(state: KGDState) -> KGDState:
    state["report"] = run_all_checks(state["_G"])
    return state

def export(state: KGDState) -> KGDState:
    state["outputs"] = export_nodes_edges_csv(state["_G"], state["out_dir"])
    Path(state["out_dir"]).mkdir(parents=True, exist_ok=True)
    (Path(state["out_dir"]) / "dq_report.json").write_text(
        __import__("json").dumps(state["report"], indent=2, default=str), encoding="utf-8"
    )
    return state

def build_app():
    g = StateGraph(KGDState)
    g.add_node("load_inputs", load_inputs)
    g.add_node("build", build)
    g.add_node("dq", dq)
    g.add_node("export", export)
    g.set_entry_point("load_inputs")
    g.add_edge("load_inputs", "build")
    g.add_edge("build", "dq")
    g.add_edge("dq", "export")
    g.add_edge("export", END)
    return g.compile()

app = build_app()
