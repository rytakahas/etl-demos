from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class Customer:
    customer_key: str


@dataclass(frozen=True)
class Dealer:
    dealer_key: str


@dataclass(frozen=True)
class Contract:
    contract_key: str
    customer_key: str
    dealer_key: Optional[str]
    approved_amount: Decimal
    term_months: int
    origination_date: Optional[date] = None
