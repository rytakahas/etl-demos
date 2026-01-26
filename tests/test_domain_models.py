from decimal import Decimal

from bankkg.domain.models import Contract


def test_contract_creation():
    c = Contract(
        contract_key="K-1",
        customer_key="C-1",
        dealer_key=None,
        approved_amount=Decimal("10000.00"),
        term_months=48,
    )
    assert c.term_months == 48
