from __future__ import annotations
from typing import Dict, Any, Optional

def score_eligibility(
    profile: Dict[str, Any],
    requested_amount: Optional[float],
    term_months: Optional[int],
    income_monthly: Optional[float],
) -> Dict[str, Any]:
    # Simple demo scoring. Replace with rules/ML.
    max_amount = 15000.0
    eligible = (requested_amount or 0.0) <= max_amount

    reason_codes = []
    reason_codes.append("HAS_HISTORY" if profile.get("contracts", 0) > 0 else "NO_HISTORY")
    reason_codes.append("AMOUNT_OK" if eligible else "AMOUNT_TOO_HIGH")

    return {
        "eligible": eligible,
        "max_amount": max_amount,
        "rate_band": "A" if profile.get("payments", 0) > 0 else "B",
        "reason_codes": reason_codes,
        "inputs": {
            "requested_amount": requested_amount,
            "term_months": term_months,
            "income_monthly": income_monthly,
        }
    }

