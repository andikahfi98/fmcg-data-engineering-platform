import argparse
import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from src.utils.database import get_connection


load_dotenv()


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_SALES_TOPIC",
    "fmcg.sales.events",
)

DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "fmcg.sales.dlq",
)

CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "fmcg-postgres-sink-v1",
)

class EventValidationError(Exception):
    """Raised when a Kafka event violates the expected data contract."""



def validate_event(event):
    required_event_fields = [
        "event_id",
        "event_type",
        "event_timestamp",
        "transaction",
    ]

    missing_event_fields = [
        field
        for field in required_event_fields
        if field not in event
    ]

    if missing_event_fields:
        raise EventValidationError(
            "Missing event fields: "
            f"{missing_event_fields}"
        )

    if event["event_type"] != "sales_transaction":
        raise EventValidationError(
            "Unsupported event type: "
            f"{event['event_type']}"
        )

    transaction = event["transaction"]

    if not isinstance(transaction, dict):
        raise EventValidationError(
            "transaction must be an object"
        )

    required_transaction_fields = [
        "date",
        "distributor_id",
        "salesperson_id",
        "outlet_id",
        "product_id",
        "invoice_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "discount_amount",
        "gross_sales",
        "net_sales",
        "unit_cogs",
        "total_cogs",
    ]

    missing_transaction_fields = [
        field
        for field in required_transaction_fields
        if field not in transaction
        or transaction[field] in (None, "")
    ]

    if missing_transaction_fields:
        raise EventValidationError(
            "Missing transaction fields: "
            f"{missing_transaction_fields}"
        )

    numeric_fields = [
        "quantity",
        "unit_price",
        "discount_pct",
        "discount_amount",
        "gross_sales",
        "net_sales",
        "unit_cogs",
        "total_cogs",
    ]

    for field in numeric_fields:
        try:
            float(transaction[field])

        except (TypeError, ValueError):
            raise EventValidationError(
                f"Invalid numeric value for "
                f"{field}: {transaction[field]}"
            )

    if int(float(transaction["quantity"])) <= 0:
        raise EventValidationError(
            "quantity must be greater than 0"
        )

    if float(transaction["gross_sales"]) < 0:
        raise EventValidationError(
            "gross_sales cannot be negative"
        )

    if float(transaction["net_sales"]) < 0:
        raise EventValidationError(
            "net_sales cannot be negative"
        )

    if float(transaction["total_cogs"]) < 0:
        raise EventValidationError(
            "total_cogs cannot be negative"
        )

def insert_event(conn, event, msg):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.sales_streaming_events (
                event_id,
                event_type,
                event_timestamp,
                source,
                source_file,
                payload,
                kafka_topic,
                kafka_partition,
                kafka_offset
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT DO NOTHING;
            """,
            (
                event["event_id"],
                event["event_type"],
                event["event_timestamp"],
                event.get("source"),
                event.get("source_file"),
                Jsonb(event),
                msg.topic(),
                msg.partition(),
                msg.offset(),
            ),
        )

        return cur.rowcount


def send_to_dlq(dlq_producer, msg, error):
    raw_value = msg.value()

    if raw_value is None:
        original_payload = None
    else:
        original_payload = raw_value.decode(
            "utf-8",
            errors="replace",
        )

    raw_key = msg.key()

    if raw_key is None:
        original_key = None
    else:
        original_key = raw_key.decode(
            "utf-8",
            errors="replace",
        )

    dlq_event = {
        "dlq_event_id": str(uuid.uuid4()),

        "failed_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "consumer_group": CONSUMER_GROUP,

        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),

        "error_type": type(error).__name__,
        "error_message": str(error),

        "original_key": original_key,
        "original_payload": original_payload,
    }

    delivery_errors = []

    def delivery_report(err, kafka_msg):
        if err is not None:
            delivery_errors.append(str(err))

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=(
            original_key.encode("utf-8")
            if original_key
            else None
        ),
        value=json.dumps(
            dlq_event,
            ensure_ascii=False,
        ).encode("utf-8"),
        callback=delivery_report,
    )

    remaining = dlq_producer.flush(10)

    if remaining != 0:
        raise RuntimeError(
            f"{remaining} DLQ message(s) "
            "were not delivered."
        )

    if delivery_errors:
        raise RuntimeError(
            f"DLQ delivery failed: "
            f"{delivery_errors}"
        )


def consume_events(max_messages=None):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    dlq_producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    consumer.subscribe([TOPIC])

    processed = 0
    inserted = 0
    duplicates = 0
    dlq_count = 0

    print("=" * 70)
    print("FMCG KAFKA -> POSTGRES CONSUMER")
    print("=" * 70)
    print(f"Broker : {BOOTSTRAP_SERVERS}")
    print(f"Topic  : {TOPIC}")
    print(f"DLQ    : {DLQ_TOPIC}")
    print(f"Group  : {CONSUMER_GROUP}")
    print(f"Limit  : {max_messages}")
    print("=" * 70)

    try:
        with get_connection() as conn:

            while True:

                if (
                    max_messages is not None
                    and processed >= max_messages
                ):
                    break

                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    raise RuntimeError(
                        f"Kafka error: {msg.error()}"
                    )

                try:
                    event = json.loads(
                        msg.value().decode("utf-8")
                    )

                    validate_event(event)

                    was_inserted = insert_event(
                        conn=conn,
                        event=event,
                        msg=msg,
                    )

                    conn.commit()

                    consumer.commit(
                        message=msg,
                        asynchronous=False,
                    )

                    processed += 1

                    if was_inserted:
                        inserted += 1
                        status = "INSERTED"
                    else:
                        duplicates += 1
                        status = "DUPLICATE"

                    transaction = event["transaction"]

                    print(
                        f"{status} | "
                        f"event={event['event_id']} | "
                        f"invoice={transaction.get('invoice_id')} | "
                        f"partition={msg.partition()} | "
                        f"offset={msg.offset()}"
                    )


                except (
                    EventValidationError,
                    json.JSONDecodeError,
                    ) as exc:

                    conn.rollback()

                    send_to_dlq(
                        dlq_producer=dlq_producer,
                        msg=msg,
                        error=exc,
                    )

                    consumer.commit(
                        message=msg,
                        asynchronous=False,
                    )

                    processed += 1
                    dlq_count += 1

                    print(
                        "DLQ | "
                        f"partition={msg.partition()} | "
                        f"offset={msg.offset()} | "
                        f"error={exc}"
                    )


                except Exception as exc:

                    conn.rollback()

                    print(
                        "SYSTEM ERROR | "
                        f"partition={msg.partition()} | "
                        f"offset={msg.offset()} | "
                        f"error={exc}"
                    )

                    # IMPORTANT:
                    # Kafka offset is NOT committed.
                    # Container restart will allow retry.
                    raise

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()
        dlq_producer.flush()

    print("=" * 70)
    print("CONSUMER FINISHED")
    print(f"Processed  : {processed}")
    print(f"Inserted   : {inserted}")
    print(f"Duplicates : {duplicates}")
    print(f"DLQ        : {dlq_count}")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    consume_events(
        max_messages=args.max_messages
    )