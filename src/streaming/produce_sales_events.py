import argparse
import csv
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_SOURCE_FILE = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "fact_sales"
    / "FactSales_2026_Q4.csv"
)

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_SALES_TOPIC",
    "fmcg.sales.events",
)


def delivery_report(err, msg):
    if err is not None:
        print(
            f"DELIVERY FAILED: {err}"
        )
        return

    print(
        "DELIVERED | "
        f"topic={msg.topic()} | "
        f"partition={msg.partition()} | "
        f"offset={msg.offset()}"
    )


def build_event(row, source_file):
    return {
        "event_id": str(uuid.uuid4()),

        "event_type": "sales_transaction",

        "event_timestamp": (
            datetime.now(timezone.utc)
            .isoformat()
        ),

        "source": "fmcg_sales_simulator",

        "source_file": source_file,

        "transaction": {
            "date": row["Date"],
            "distributor_id": row["DistributorID"],
            "salesperson_id": row["SalespersonID"],
            "outlet_id": row["OutletID"],
            "product_id": row["ProductID"],
            "invoice_id": row["InvoiceID"],

            "quantity": row["Quantity"],
            "unit_price": row["UnitPrice"],
            "discount_pct": row["DiscountPct"],
            "discount_amount": row["DiscountAmount"],
            "gross_sales": row["GrossSales"],
            "net_sales": row["NetSales"],
            "unit_cogs": row["UnitCOGS"],
            "total_cogs": row["TotalCOGS"],
        },
    }


def produce_events(
    source_file,
    limit=None,
    interval=1.0,
):
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,

            # Reliable producer configuration
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    produced = 0

    print("=" * 70)
    print("FMCG KAFKA SALES PRODUCER")
    print("=" * 70)
    print(f"Broker : {BOOTSTRAP_SERVERS}")
    print(f"Topic  : {TOPIC}")
    print(f"Source : {source_file}")
    print(f"Limit  : {limit}")
    print(f"Delay  : {interval} second(s)")
    print("=" * 70)

    with open(
        source_file,
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            if limit is not None and produced >= limit:
                break

            event = build_event(
                row=row,
                source_file=Path(source_file).name,
            )

            invoice_id = row["InvoiceID"]

            producer.produce(
                topic=TOPIC,

                # Same invoice tends to go to same partition
                key=invoice_id.encode("utf-8"),

                value=json.dumps(
                    event,
                    ensure_ascii=False,
                ).encode("utf-8"),

                callback=delivery_report,
            )

            producer.poll(0)

            produced += 1

            print(
                f"EVENT {produced} SENT | "
                f"invoice={invoice_id} | "
                f"product={row['ProductID']} | "
                f"net_sales={row['NetSales']}"
            )

            if interval > 0:
                time.sleep(interval)

    remaining = producer.flush()

    if remaining != 0:
        raise RuntimeError(
            f"{remaining} Kafka messages "
            "were not delivered."
        )

    print("=" * 70)
    print(
        f"PRODUCER COMPLETED | "
        f"{produced} event(s) sent"
    )
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source-file",
        default=str(DEFAULT_SOURCE_FILE),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
    )

    args = parser.parse_args()

    produce_events(
        source_file=args.source_file,
        limit=args.limit,
        interval=args.interval,
    )