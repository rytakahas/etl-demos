from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from bankkg.kgd_networkx.dq_policy import Violation


def render_markdown_report(
    run_meta: Dict[str, Any],
    metrics: Dict[str, Any],
    checks: Dict[str, Any],
    ok: bool,
    violations: List[Violation],
    drift: Optional[Dict[str, Any]] = None,
) -> str:
    ts = run_meta.get("run_ts") or datetime.utcnow().isoformat()
    run_id = run_meta.get("run_id", "unknown")
    git_sha = run_meta.get("git_sha", "unknown")

    lines: List[str] = []
    lines.append("# KGD Graph DQ Report")
    lines.append("")
    lines.append(f"- **run_id**: `{run_id}`")
    lines.append(f"- **run_ts**: `{ts}`")
    lines.append(f"- **git_sha**: `{git_sha}`")
    lines.append(f"- **status**: {'✅ PASS' if ok else '❌ FAIL'}")
    lines.append("")

    lines.append("## Summary metrics")
    lines.append("")
    lines.append(f"- nodes: **{metrics.get('nodes')}**")
    lines.append(f"- edges: **{metrics.get('edges')}**")
    lines.append(f"- orphan_edges: **{metrics.get('orphan_edges')}** ({metrics.get('orphan_edges_pct',0.0):.6f})")
    lines.append(f"- unknown_node_pct: **{metrics.get('unknown_node_pct',0.0):.6f}**")
    lines.append(f"- components: **{metrics.get('components')}**")
    lines.append(f"- largest_component_share: **{metrics.get('largest_component_share',0.0):.6f}**")
    lines.append(f"- max_degree: **{metrics.get('max_degree')}**")
    lines.append("")

    lines.append("## Top node labels")
    lines.append("")
    for lbl, cnt in (checks.get("node_labels_top", []) or [])[:20]:
        lines.append(f"- {lbl}: {cnt}")
    lines.append("")

    lines.append("## Top edge types")
    lines.append("")
    for t, cnt in (checks.get("edge_types_top", []) or [])[:20]:
        lines.append(f"- {t}: {cnt}")
    lines.append("")

    lines.append("## Supernodes (degree)")
    lines.append("")
    supernodes = checks.get("supernodes", []) or []
    if not supernodes:
        lines.append("- none")
    else:
        for n, deg in supernodes[:20]:
            lines.append(f"- `{n}`: {deg}")
    lines.append("")

    if violations:
        lines.append("## Policy violations")
        lines.append("")
        for v in violations:
            lines.append(f"- **{v.metric}** {v.value} violates {v.op} {v.threshold} — {v.message}")
        lines.append("")

    if drift:
        lines.append("## Drift vs previous run")
        lines.append("")
        for k, v in drift.get("deltas", {}).items():
            lines.append(f"- {k}: {v:+.6f}")
        lines.append("")
        if drift.get("flags"):
            lines.append("### Drift flags")
            for f in drift["flags"]:
                lines.append(f"- {f}")
            lines.append("")

    return "\n".join(lines)
