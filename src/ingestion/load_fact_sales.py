import csv
from pathlib import Path

from src.utils.database import get_connection


BASE_DIR = Path(__file__).resolve().parents[2]
FACT_SALES_DIR = BASE_DIR / "data" / "source" / "fact_sales"


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


RAW_COLUMNS = [
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


def start_pipeline_run(conn, source_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.pipeline_runs (
                pipeline_name,
                source_name,
                status
            )
            VALUES (
                %s,
                %s,
                'RUNNING'
            )
            RETURNING run_id;
            """,
            ("fact_sales_ingestion", source_name),
        )

        return cur.fetchone()[0]


def complete_pipeline_run(
    conn,
    run_id,
    records_received,
    records_inserted,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE metadata.pipeline_runs
            SET
                finished_at = CURRENT_TIMESTAMP,
                records_received = %s,
                records_inserted = %s,
                status = 'SUCCESS'
            WHERE run_id = %s;
            """,
            (
                records_received,
                records_inserted,
                run_id,
            ),
        )


def fail_pipeline_run(conn, run_id, error_message):
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


def validate_header(header):
    if header is None:
        raise ValueError("CSV file does not contain a header.")

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

def is_file_already_processed(conn, source_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM metadata.pipeline_runs
                WHERE pipeline_name = %s
                  AND source_name = %s
                  AND status = 'SUCCESS'
            );
            """,
            (
                "fact_sales_ingestion",
                source_name,
            ),
        )

        return cur.fetchone()[0]

def ingest_fact_sales(file_path):
    print("=" * 70)
    print("FACT SALES INGESTION")
    print("=" * 70)
    print(f"Source : {file_path.name}")

    with get_connection() as conn:

        if is_file_already_processed(conn, file_path.name):
            print("=" * 70)
            print("FACT SALES INGESTION")
            print("=" * 70)
            print(f"Source : {file_path.name}")
            print("Status : SKIPPED (already processed)")
            return
        run_id = start_pipeline_run(
            conn,
            file_path.name,
        )

        conn.commit()

        print(f"Run ID : {run_id}")

        try:
            records_received = 0

            with file_path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as file:

                reader = csv.DictReader(file)

                validate_header(reader.fieldnames)

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
                    with cur.copy(copy_sql) as copy:

                        for source_row_number, row in enumerate(
                            reader,
                            start=2,
                        ):
                            values = [
                                row[column]
                                for column in EXPECTED_COLUMNS
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

            complete_pipeline_run(
                conn,
                run_id,
                records_received,
                records_received,
            )

            conn.commit()

            print(f"Rows   : {records_received:,}")
            print("Status : SUCCESS")

        except Exception as exc:
            conn.rollback()

            # Record the failed run separately
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


if __name__ == "__main__":
    source_files = sorted(
        FACT_SALES_DIR.glob("FactSales_*.csv")
    )

    print(f"Found {len(source_files)} source files.")

    for source_file in source_files:
        ingest_fact_sales(source_file)