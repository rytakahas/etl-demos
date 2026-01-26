from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


@dataclass(frozen=True)
class Violation:
    metric: str
    value: float
    threshold: float
    op: str  # "max" or "min"
    message: str


def load_policy(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Policy file not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def evaluate_policy(metrics: Dict[str, Any], policy: Dict[str, Any]) -> Tuple[bool, List[Violation]]:
    thresholds = (policy or {}).get("thresholds", {}) or {}
    violations: List[Violation] = []

    def check_max(key: str, thresh: float, msg: str):
        v = float(metrics.get(key, 0.0))
        if v > float(thresh):
            violations.append(Violation(key, v, float(thresh), "max", msg))

    def check_min(key: str, thresh: float, msg: str):
        v = float(metrics.get(key, 0.0))
        if v < float(thresh):
            violations.append(Violation(key, v, float(thresh), "min", msg))

    if "orphan_edges_pct_max" in thresholds:
        check_max("orphan_edges_pct", thresholds["orphan_edges_pct_max"], "Too many orphan/placeholder edges")
    if "unknown_node_pct_max" in thresholds:
        check_max("unknown_node_pct", thresholds["unknown_node_pct_max"], "Too many UNKNOWN/placeholder nodes")
    if "components_max" in thresholds:
        check_max("components", thresholds["components_max"], "Too many connected components (graph fragmentation)")
    if "largest_component_share_min" in thresholds:
        check_min("largest_component_share", thresholds["largest_component_share_min"], "Largest component too small (fragmented)")
    if "supernode_degree_max" in thresholds:
        check_max("max_degree", thresholds["supernode_degree_max"], "Supernode degree too high (performance risk)")

    ok = len(violations) == 0
    return ok, violations
