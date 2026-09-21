import argparse

from src.utils.database import get_connection


DAG_ID = "fmcg_batch_pipeline"


def get_count(cur, table_name):
    cur.execute(
        f"SELECT COUNT(*) FROM {table_name};"
    )
    return cur.fetchone()[0]


def insert_audit(
    conn,
    dag_run_id,
    counts,
    status,
    message,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.batch_pipeline_audit (
                dag_id,
                dag_run_id,

                raw_sales_rows,
                staging_sales_rows,

                raw_streaming_sales_rows,
                staging_streaming_sales_rows,

                unified_sales_rows,
                warehouse_sales_rows,

                raw_inventory_rows,
                staging_inventory_rows,
                warehouse_inventory_rows,

                raw_target_rows,
                staging_target_rows,
                warehouse_target_rows,

                validation_status,
                validation_message
            )
            VALUES (
                %s, %s,

                %s, %s,

                %s, %s,

                %s, %s,

                %s, %s, %s,

                %s, %s, %s,

                %s, %s
            );
            """,
            (
                DAG_ID,
                dag_run_id,

                counts["raw_sales"],
                counts["staging_sales"],

                counts["raw_streaming_sales"],
                counts["staging_streaming_sales"],

                counts["unified_sales"],
                counts["warehouse_sales"],

                counts["raw_inventory"],
                counts["staging_inventory"],
                counts["warehouse_inventory"],

                counts["raw_target"],
                counts["staging_target"],
                counts["warehouse_target"],

                status,
                message,
            ),
        )


def validate_pipeline(dag_run_id):

    with get_connection() as conn:

        with conn.cursor() as cur:

            counts = {
                "raw_sales":
                    get_count(
                        cur,
                        "raw.current_sales",
                    ),

                "staging_sales":
                    get_count(
                        cur,
                        "staging.stg_sales",
                    ),

                "raw_streaming_sales":
                    get_count(
                        cur,
                        "raw.sales_streaming_events",
                    ),

                "staging_streaming_sales":
                    get_count(
                        cur,
                        "staging.stg_sales_streaming",
                    ),

                "unified_sales":
                    get_count(
                        cur,
                        "staging.stg_sales_unified",
                    ),

                "warehouse_sales":
                    get_count(
                        cur,
                        "warehouse.fact_sales",
                    ),

                "raw_inventory":
                    get_count(
                        cur,
                        "raw.current_inventory",
                    ),

                "staging_inventory":
                    get_count(
                        cur,
                        "staging.stg_inventory",
                    ),

                "warehouse_inventory":
                    get_count(
                        cur,
                        "warehouse.fact_inventory",
                    ),

                "raw_target":
                    get_count(
                        cur,
                        "raw.current_target",
                    ),

                "staging_target":
                    get_count(
                        cur,
                        "staging.stg_target",
                    ),

                "warehouse_target":
                    get_count(
                        cur,
                        "warehouse.fact_sales_target",
                    ),
            }

        problems = []

        # Batch reconciliation
        if (
            counts["raw_sales"]
            != counts["staging_sales"]
        ):
            problems.append(
                "Batch sales RAW and staging "
                "row counts do not match."
            )

        # Streaming reconciliation
        if (
            counts["raw_streaming_sales"]
            != counts["staging_streaming_sales"]
        ):
            problems.append(
                "Streaming sales RAW and staging "
                "row counts do not match."
            )

        # Unified → warehouse reconciliation
        if (
            counts["unified_sales"]
            != counts["warehouse_sales"]
        ):
            problems.append(
                "Unified sales and warehouse "
                "row counts do not match."
            )

        # Inventory
        if not (
            counts["raw_inventory"]
            == counts["staging_inventory"]
            == counts["warehouse_inventory"]
        ):
            problems.append(
                "Inventory row counts do not match."
            )

        # Target
        if not (
            counts["raw_target"]
            == counts["staging_target"]
            == counts["warehouse_target"]
        ):
            problems.append(
                "Target row counts do not match."
            )

        if problems:
            status = "FAILED"
            message = " ".join(problems)

        else:
            status = "SUCCESS"
            message = (
                "Batch, streaming, unified, "
                "and warehouse reconciliation "
                "completed successfully."
            )

        insert_audit(
            conn=conn,
            dag_run_id=dag_run_id,
            counts=counts,
            status=status,
            message=message,
        )

        conn.commit()

    print("=" * 70)
    print("PIPELINE VALIDATION")
    print("=" * 70)

    print(
        f"Batch Sales     : "
        f"{counts['raw_sales']:,} / "
        f"{counts['staging_sales']:,}"
    )

    print(
        f"Streaming Sales : "
        f"{counts['raw_streaming_sales']:,} / "
        f"{counts['staging_streaming_sales']:,}"
    )

    print(
        f"Unified Sales   : "
        f"{counts['unified_sales']:,}"
    )

    print(
        f"Warehouse Sales : "
        f"{counts['warehouse_sales']:,}"
    )

    print(
        f"Inventory       : "
        f"{counts['raw_inventory']:,} / "
        f"{counts['staging_inventory']:,} / "
        f"{counts['warehouse_inventory']:,}"
    )

    print(
        f"Target          : "
        f"{counts['raw_target']:,} / "
        f"{counts['staging_target']:,} / "
        f"{counts['warehouse_target']:,}"
    )

    print(f"Status          : {status}")
    print(f"Message         : {message}")

    if status != "SUCCESS":
        raise RuntimeError(
            "Pipeline validation failed."
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dag-run-id",
        required=True,
    )

    args = parser.parse_args()

    validate_pipeline(
        dag_run_id=args.dag_run_id
    )