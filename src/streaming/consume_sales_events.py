import argparse
import json
import os

from confluent_kafka import Consumer
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

CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "fmcg-postgres-sink-v1",
)


def validate_event(event):
    required_fields = [
        "event_id",
        "event_type",
        "event_timestamp",
        "transaction",
    ]

    missing = [
        field
        for field in required_fields
        if field not in event
    ]

    if missing:
        raise ValueError(
            f"Missing required fields: {missing}"
        )

    if event["event_type"] != "sales_transaction":
        raise ValueError(
            f"Unsupported event type: "
            f"{event['event_type']}"
        )

    if not isinstance(
        event["transaction"],
        dict,
    ):
        raise ValueError(
            "transaction must be an object"
        )


def insert_event(
    conn,
    event,
    msg,
):
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

        inserted = cur.rowcount

    return inserted


def consume_events(max_messages=None):
    consumer = Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                CONSUMER_GROUP,

            # For a brand-new consumer group,
            # begin with existing events.
            "auto.offset.reset":
                "earliest",

            # We commit only AFTER PostgreSQL commit.
            "enable.auto.commit":
                False,
        }
    )

    consumer.subscribe([TOPIC])

    processed = 0
    inserted = 0
    duplicates = 0

    print("=" * 70)
    print("FMCG KAFKA → POSTGRES CONSUMER")
    print("=" * 70)
    print(
        f"Broker : {BOOTSTRAP_SERVERS}"
    )
    print(
        f"Topic  : {TOPIC}"
    )
    print(
        f"Group  : {CONSUMER_GROUP}"
    )
    print(
        f"Limit  : {max_messages}"
    )
    print("=" * 70)

    try:
        with get_connection() as conn:

            while True:

                if (
                    max_messages is not None
                    and processed >= max_messages
                ):
                    break

                msg = consumer.poll(
                    timeout=1.0
                )

                if msg is None:
                    continue

                if msg.error():
                    raise RuntimeError(
                        f"Kafka error: "
                        f"{msg.error()}"
                    )

                try:
                    event = json.loads(
                        msg.value().decode(
                            "utf-8"
                        )
                    )

                    validate_event(event)

                    was_inserted = insert_event(
                        conn=conn,
                        event=event,
                        msg=msg,
                    )

                    # PostgreSQL first
                    conn.commit()

                    # Kafka offset second
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

                    transaction = (
                        event["transaction"]
                    )

                    print(
                        f"{status} | "
                        f"event={event['event_id']} | "
                        f"invoice="
                        f"{transaction.get('invoice_id')} | "
                        f"partition={msg.partition()} | "
                        f"offset={msg.offset()}"
                    )

                except Exception as exc:
                    conn.rollback()

                    print(
                        "MESSAGE FAILED | "
                        f"partition={msg.partition()} | "
                        f"offset={msg.offset()} | "
                        f"error={exc}"
                    )

                    raise

    except KeyboardInterrupt:
        print(
            "\nConsumer stopped by user."
        )

    finally:
        consumer.close()

    print("=" * 70)
    print("CONSUMER FINISHED")
    print(f"Processed  : {processed}")
    print(f"Inserted   : {inserted}")
    print(f"Duplicates : {duplicates}")
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