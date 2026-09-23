import json
import os
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


# Event ID sengaja FIXED agar duplicate bisa diuji.
EVENT_ID = "duplicate-test-event-0001"


event = {
    "event_id": EVENT_ID,
    "event_type": "sales_transaction",
    "event_timestamp": datetime.now(
        timezone.utc
    ).isoformat(),

    "source": "duplicate_integration_test",
    "source_file": None,

    "transaction": {
        "date": "2026-09-21",
        "distributor_id": "DST001",
        "salesperson_id": "SP002",
        "outlet_id": "OUT00002",
        "product_id": "BV008",
        "invoice_id": "INV_DUPLICATE_TEST_0001",

        "quantity": "2",
        "unit_price": "13000",
        "discount_pct": "0.0500",
        "discount_amount": "1300",
        "gross_sales": "26000",
        "net_sales": "24700",
        "unit_cogs": "6000",
        "total_cogs": "12000",
    },
}


producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    }
)


# Sengaja kirim event yang sama 2 kali.
for number in range(1, 3):

    producer.produce(
        topic=TOPIC,
        key=event["transaction"][
            "invoice_id"
        ].encode("utf-8"),

        value=json.dumps(
            event
        ).encode("utf-8"),
    )

    print(
        f"Duplicate copy {number} sent"
    )


remaining = producer.flush()

if remaining != 0:
    raise RuntimeError(
        f"{remaining} message(s) not delivered."
    )


print("Duplicate test completed.")