from __future__ import annotations
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from bankkg.langgraph.graph import app as lg_app

api = FastAPI(title="BankKG LangGraph API")

class LoanQuery(BaseModel):
    user_question: str
    customer_id: Optional[str] = None
    requested_amount: Optional[float] = None
    term_months: Optional[int] = None
    income_monthly: Optional[float] = None

@api.get("/health")
def health():
    return {"status": "ok"}

@api.post("/loan/qualify")
def qualify(q: LoanQuery):
    state = lg_app.invoke(q.model_dump())
    return {
        "answer": state.get("answer", ""),
        "decision": state.get("decision", {}),
        "profile": state.get("profile", {}),
        "policy_chunks": state.get("policy_chunks", []),
    }

