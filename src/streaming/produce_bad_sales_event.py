import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_SALES_TOPIC",
    "fmcg.sales.events",
)


# Intentionally invalid:
# transaction field is missing.

event = {
    "event_id": str(uuid.uuid4()),
    "event_type": "sales_transaction",
    "event_timestamp": datetime.now(
        timezone.utc
    ).isoformat(),
    "source": "dlq_integration_test",
    "transaction": {
    "date": "2026-09-21",
    "distributor_id": "DST001",
    "salesperson_id": "SP002",
    "outlet_id": "OUT00002",
    "product_id": "BV008",
    "invoice_id": "INV_BAD_0002",

    "quantity": "ABC",

    "unit_price": "13000",
    "discount_pct": "0.0500",
    "discount_amount": "1300",
    "gross_sales": "26000",
    "net_sales": "24700",
    "unit_cogs": "6000",
    "total_cogs": "12000"
}
}


producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    }
)


producer.produce(
    topic=TOPIC,
    key=b"DLQ_TEST_0001",
    value=json.dumps(event).encode("utf-8"),
)

remaining = producer.flush()

if remaining != 0:
    raise RuntimeError(
        f"{remaining} message(s) were not delivered."
    )


print("=" * 60)
print("INVALID TEST EVENT SENT")
print("=" * 60)
print(f"Topic    : {TOPIC}")
print(f"Event ID : {event['event_id']}")
print("Expected : event should be routed to DLQ")