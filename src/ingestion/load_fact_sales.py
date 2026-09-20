import csv
from pathlib import Path

from src.utils.database import get_connection
from src.utils.file_metadata import get_file_metadata


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]

FACT_SALES_DIR = (
    BASE_DIR
    / "data"
    / "source"
    / "fact_sales"
)

PIPELINE_NAME = "fact_sales_ingestion"


EXPECTED_COLUMNS = [
    "Date",
    "DistributorID",
    "SalespersonID",
    "OutletID",
    "ProductID",
    "InvoiceID",
    "Quantity",
    "UnitPrice",
    "DiscountPct",
    "DiscountAmount",
    "GrossSales",
    "NetSales",
    "UnitCOGS",
    "TotalCOGS",
]


# ============================================================
# PIPELINE METADATA
# ============================================================

def start_pipeline_run(
    conn,
    source_name,
    file_metadata,
):
    """
    Create a RUNNING pipeline record and return run_id.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.pipeline_runs (
                pipeline_name,
                source_name,
                source_hash,
                source_size_bytes,
                source_modified_at,
                status
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                'RUNNING'
            )
            RETURNING run_id;
            """,
            (
                PIPELINE_NAME,
                source_name,
                file_metadata.file_hash,
                file_metadata.file_size_bytes,
                file_metadata.source_modified_at,
            ),
        )

        run_id = cur.fetchone()[0]

    return run_id


def complete_pipeline_run(
    conn,
    run_id,
    records_received,
    records_inserted,
):
    """
    Mark pipeline run as SUCCESS.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE metadata.pipeline_runs
            SET
                finished_at = CURRENT_TIMESTAMP,
                records_received = %s,
                records_inserted = %s,
                records_rejected = 0,
                status = 'SUCCESS'
            WHERE run_id = %s;
            """,
            (
                records_received,
                records_inserted,
                run_id,
            ),
        )


def fail_pipeline_run(
    conn,
    run_id,
    error_message,
):
    """
    Mark pipeline run as FAILED.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE metadata.pipeline_runs
            SET
                finished_at = CURRENT_TIMESTAMP,
                status = 'FAILED',
                error_message = %s
            WHERE run_id = %s;
            """,
            (
                str(error_message),
                run_id,
            ),
        )


# ============================================================
# IDEMPOTENCY
# ============================================================

def get_processed_run_id(
    conn,
    source_name,
    source_hash,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM metadata.pipeline_runs
            WHERE pipeline_name = %s
              AND source_name = %s
              AND source_hash = %s
              AND status = 'SUCCESS'
            ORDER BY run_id DESC
            LIMIT 1;
            """,
            (
                PIPELINE_NAME,
                source_name,
                source_hash,
            ),
        )

        result = cur.fetchone()

        if result is None:
            return None

        return result[0]

def set_source_state(
    conn,
    source_name,
    source_hash,
    active_run_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.source_state (
                pipeline_name,
                source_name,
                active_run_id,
                active_source_hash,
                last_observed_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                CURRENT_TIMESTAMP
            )

            ON CONFLICT (
                pipeline_name,
                source_name
            )

            DO UPDATE SET
                active_run_id = EXCLUDED.active_run_id,
                active_source_hash = EXCLUDED.active_source_hash,
                last_observed_at = CURRENT_TIMESTAMP;
            """,
            (
                PIPELINE_NAME,
                source_name,
                active_run_id,
                source_hash,
            ),
        )

# ============================================================
# SCHEMA VALIDATION
# ============================================================

def validate_header(header):
    """
    Validate CSV schema.

    Column order is intentionally ignored.
    Required column names must still match.
    """

    if header is None:
        raise ValueError(
            "CSV file does not contain a header."
        )

    missing_columns = [
        column
        for column in EXPECTED_COLUMNS
        if column not in header
    ]

    unexpected_columns = [
        column
        for column in header
        if column not in EXPECTED_COLUMNS
    ]

    if missing_columns or unexpected_columns:
        raise ValueError(
            f"""
Unexpected CSV schema.

Missing columns:
{missing_columns}

Unexpected columns:
{unexpected_columns}

Expected:
{EXPECTED_COLUMNS}

Received:
{header}
"""
        )


# ============================================================
# INGESTION
# ============================================================

def ingest_fact_sales(file_path):
    """
    Ingest one Fact Sales CSV file into raw.sales.
    """

    file_metadata = get_file_metadata(
        file_path
    )

    print()
    print("=" * 70)
    print("FACT SALES INGESTION")
    print("=" * 70)
    print(f"Source : {file_path.name}")
    print(
        f"Hash   : "
        f"{file_metadata.file_hash[:12]}..."
    )

    with get_connection() as conn:

        # ----------------------------------------------------
        # IDEMPOTENCY CHECK
        # ----------------------------------------------------

        processed_run_id = get_processed_run_id(
    conn,
    file_path.name,
    file_metadata.file_hash,
)

        if processed_run_id is not None:

            set_source_state(
                conn,
                file_path.name,
                file_metadata.file_hash,
                processed_run_id,
            )

            conn.commit()

            print(
                "Status : SKIPPED "
                "(same file version already processed)"
            )

            print(
                f"Active Run ID : "
                f"{processed_run_id}"
            )

            return

        # ----------------------------------------------------
        # START PIPELINE RUN
        # ----------------------------------------------------

        run_id = start_pipeline_run(
            conn,
            file_path.name,
            file_metadata,
        )

        # Persist RUNNING status separately from
        # the ingestion transaction.
        conn.commit()

        print(f"Run ID : {run_id}")

        try:

            records_received = 0

            # ------------------------------------------------
            # OPEN CSV
            # ------------------------------------------------

            with file_path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as file:

                reader = csv.DictReader(file)

                if reader.fieldnames is None:
                    raise ValueError(
                        "CSV file does not contain a header."
                    )

                # Normalize possible whitespace in header.
                reader.fieldnames = [
                    column.strip()
                    for column in reader.fieldnames
                ]

                validate_header(
                    reader.fieldnames
                )

                # ------------------------------------------------
                # POSTGRESQL COPY
                # ------------------------------------------------

                copy_sql = """
                    COPY raw.sales (
                        date,
                        distributor_id,
                        salesperson_id,
                        outlet_id,
                        product_id,
                        invoice_id,
                        quantity,
                        unit_price,
                        discount_pct,
                        discount_amount,
                        gross_sales,
                        net_sales,
                        unit_cogs,
                        total_cogs,
                        source_file,
                        source_row_number,
                        pipeline_run_id
                    )
                    FROM STDIN
                """

                with conn.cursor() as cur:

                    with cur.copy(
                        copy_sql
                    ) as copy:

                        for (
                            source_row_number,
                            row,
                        ) in enumerate(
                            reader,
                            start=2,
                        ):

                            values = [
                                row[column]
                                for column
                                in EXPECTED_COLUMNS
                            ]

                            copy.write_row(
                                (
                                    *values,
                                    file_path.name,
                                    source_row_number,
                                    run_id,
                                )
                            )

                            records_received += 1

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            complete_pipeline_run(
                conn,
                run_id,
                records_received,
                records_received,
            )

            set_source_state(
                conn,
                file_path.name,
                file_metadata.file_hash,
                run_id,
            )

            conn.commit()

            print(
                f"Rows   : "
                f"{records_received:,}"
            )

            print("Status : SUCCESS")

        except Exception as exc:

            # Roll back raw inserts from failed load.
            conn.rollback()

            # RUNNING record was already committed,
            # so update its state using another connection.
            with get_connection() as error_conn:

                fail_pipeline_run(
                    error_conn,
                    run_id,
                    exc,
                )

                error_conn.commit()

            print("Status : FAILED")
            print(f"Error  : {exc}")

            raise


# ============================================================
# BATCH PROCESSING
# ============================================================

def ingest_all_fact_sales():
    """
    Discover and ingest all FactSales CSV files.
    """

    source_files = sorted(
        FACT_SALES_DIR.glob(
            "FactSales_*.csv"
        )
    )

    if not source_files:
        raise FileNotFoundError(
            f"No Fact Sales files found in: "
            f"{FACT_SALES_DIR}"
        )

    print()
    print("=" * 70)
    print("FACT SALES BATCH PIPELINE")
    print("=" * 70)
    print(
        f"Files found : "
        f"{len(source_files)}"
    )

    for source_file in source_files:
        ingest_fact_sales(
            source_file
        )

    print()
    print("=" * 70)
    print("BATCH PIPELINE COMPLETE")
    print("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    ingest_all_fact_sales()