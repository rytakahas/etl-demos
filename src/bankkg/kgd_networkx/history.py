from __future__ import annotations

from typing import Any, Dict, List, Optional
from pathlib import Path
import json


def load_prev_report(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def compute_drift(prev: Optional[Dict[str, Any]], current_metrics: Dict[str, Any]) -> Dict[str, Any]:
    if not prev:
        return {"deltas": {}, "flags": []}

    prev_metrics = (prev.get("metrics") or {})
    deltas: Dict[str, float] = {}
    flags: List[str] = []

    def delta(key: str) -> float:
        try:
            return float(current_metrics.get(key, 0.0)) - float(prev_metrics.get(key, 0.0))
        except Exception:
            return 0.0

    for key in ["nodes", "edges", "orphan_edges_pct", "unknown_node_pct", "components", "largest_component_share", "max_degree"]:
        deltas[key] = delta(key)

    # crude flags (tune later)
    if abs(deltas["nodes"]) > 0.05 * max(float(prev_metrics.get("nodes", 1.0)), 1.0):
        flags.append("Node count changed >5%")
    if abs(deltas["edges"]) > 0.05 * max(float(prev_metrics.get("edges", 1.0)), 1.0):
        flags.append("Edge count changed >5%")
    if abs(deltas["orphan_edges_pct"]) > 0.001:
        flags.append("Orphan edge ratio drift >0.1%")
    if abs(deltas["largest_component_share"]) > 0.05:
        flags.append("Largest component share drift >5%")

    return {"deltas": deltas, "flags": flags}
