from __future__ import annotations
from typing import TypedDict, Optional, Dict, Any, List

class LoanState(TypedDict, total=False):
    user_question: str
    customer_id: Optional[str]
    requested_amount: Optional[float]
    term_months: Optional[int]
    income_monthly: Optional[float]

    profile: Dict[str, Any]               # from Neo4j
    policy_chunks: List[Dict[str, Any]]   # retrieved chunks with citations
    decision: Dict[str, Any]              # scoring output
    answer: str
    citations_ok: bool
    attempts: int

