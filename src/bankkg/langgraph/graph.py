from __future__ import annotations
from langgraph.graph import StateGraph, END
from bankkg.langgraph.state import LoanState
from bankkg.langgraph.tools_neo4j import get_customer_profile, retrieve_policy_chunks
from bankkg.langgraph.scoring import score_eligibility

def parse_request(state: LoanState) -> LoanState:
    state.setdefault("attempts", 0)
    return state

def profile_node(state: LoanState) -> LoanState:
    cid = state.get("customer_id") or ""
    state["profile"] = get_customer_profile(cid) if cid else {"customer_id": None, "contracts": 0, "payments": 0, "evidence_paths": []}
    return state

def policy_node(state: LoanState) -> LoanState:
    state["policy_chunks"] = retrieve_policy_chunks(state.get("user_question", ""), k=3)
    return state

def score_node(state: LoanState) -> LoanState:
    state["decision"] = score_eligibility(
        profile=state.get("profile", {}),
        requested_amount=state.get("requested_amount"),
        term_months=state.get("term_months"),
        income_monthly=state.get("income_monthly"),
    )
    return state

def explain_node(state: LoanState) -> LoanState:
    d = state.get("decision", {})
    prof = state.get("profile", {})
    chunks = state.get("policy_chunks", [])
    evidence_graph = prof.get("evidence_paths", [])
    chunk_cite = chunks[0]["chunk_id"] if chunks else "none"

    state["answer"] = (
        f"Eligibility: {d.get('eligible')}\n"
        f"Max eligible amount (estimate): {d.get('max_amount')}\n"
        f"Rate band: {d.get('rate_band')}\n\n"
        f"Reasons: {', '.join(d.get('reason_codes', []))}\n\n"
        f"Evidence (graph): {evidence_graph[0] if evidence_graph else 'none'}\n"
        f"Evidence (policy): {chunk_cite}\n"
    )
    return state

def check_node(state: LoanState) -> LoanState:
    ans = state.get("answer", "")
    ok = ("Evidence (graph):" in ans) and ("Evidence (policy):" in ans)
    state["citations_ok"] = ok
    return state

def route_after_check(state: LoanState) -> str:
    if state.get("citations_ok"):
        return "done"
    if state.get("attempts", 0) >= 1:
        return "done"
    state["attempts"] = state.get("attempts", 0) + 1
    return "retry"

def build_app():
    g = StateGraph(LoanState)
    g.add_node("parse", parse_request)
    g.add_node("profile", profile_node)
    g.add_node("policy", policy_node)
    g.add_node("score", score_node)
    g.add_node("explain", explain_node)
    g.add_node("check", check_node)

    g.set_entry_point("parse")
    g.add_edge("parse", "profile")
    g.add_edge("profile", "policy")
    g.add_edge("policy", "score")
    g.add_edge("score", "explain")
    g.add_edge("explain", "check")

    g.add_conditional_edges("check", route_after_check, {"retry": "profile", "done": END})
    return g.compile()

app = build_app()

