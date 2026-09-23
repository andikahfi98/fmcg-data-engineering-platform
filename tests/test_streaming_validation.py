import pytest

from src.streaming.consume_sales_events import (
    EventValidationError,
    validate_event,
)


def valid_event():
    return {
        "event_id": "test-event-001",
        "event_type": "sales_transaction",
        "event_timestamp": "2026-09-21T15:00:00+00:00",
        "source": "pytest",

        "transaction": {
            "date": "2026-09-21",
            "distributor_id": "DST001",
            "salesperson_id": "SP002",
            "outlet_id": "OUT00002",
            "product_id": "BV008",
            "invoice_id": "INV_TEST_001",

            "quantity": "2",
            "unit_price": "13000",
            "discount_pct": "0.05",
            "discount_amount": "1300",
            "gross_sales": "26000",
            "net_sales": "24700",
            "unit_cogs": "6000",
            "total_cogs": "12000",
        },
    }


def test_valid_event_passes():
    event = valid_event()

    validate_event(event)


def test_missing_transaction_goes_invalid():
    event = valid_event()

    del event["transaction"]

    with pytest.raises(
        EventValidationError,
        match="Missing event fields",
    ):
        validate_event(event)


def test_missing_invoice_id_goes_invalid():
    event = valid_event()

    del event["transaction"]["invoice_id"]

    with pytest.raises(
        EventValidationError,
        match="Missing transaction fields",
    ):
        validate_event(event)


def test_invalid_quantity_goes_invalid():
    event = valid_event()

    event["transaction"]["quantity"] = "ABC"

    with pytest.raises(
        EventValidationError,
        match="Invalid numeric value",
    ):
        validate_event(event)


def test_zero_quantity_goes_invalid():
    event = valid_event()

    event["transaction"]["quantity"] = "0"

    with pytest.raises(
        EventValidationError,
        match="quantity must be greater than 0",
    ):
        validate_event(event)


def test_negative_net_sales_goes_invalid():
    event = valid_event()

    event["transaction"]["net_sales"] = "-100"

    with pytest.raises(
        EventValidationError,
        match="net_sales cannot be negative",
    ):
        validate_event(event)


def test_wrong_event_type_goes_invalid():
    event = valid_event()

    event["event_type"] = "inventory_transaction"

    with pytest.raises(
        EventValidationError,
        match="Unsupported event type",
    ):
        validate_event(event)


def test_transaction_must_be_object():
    event = valid_event()

    event["transaction"] = "invalid"

    with pytest.raises(
        EventValidationError,
        match="transaction must be an object",
    ):
        validate_event(event)