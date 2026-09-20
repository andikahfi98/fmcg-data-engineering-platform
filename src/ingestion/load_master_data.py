from pathlib import Path

import pandas as pd

from src.utils.database import get_connection


BASE_DIR = Path(__file__).resolve().parents[2]

MASTER_FILE = (
    BASE_DIR
    / "data"
    / "source"
    / "KopDes_MasterData.xlsx"
)


SHEET_CONFIG = {
    "DimProduct": {
        "table": "raw.product",
        "source_columns": [
            "ProductID",
            "Category",
            "Brand",
            "ProductName",
            "Variant",
            "PackSize",
            "UnitPrice",
            "UnitCOGS",
        ],
        "raw_columns": [
            "product_id",
            "category",
            "brand",
            "product_name",
            "variant",
            "pack_size",
            "unit_price",
            "unit_cogs",
        ],
    },

    "DimDistributor": {
        "table": "raw.distributor",
        "source_columns": [
            "DistributorID",
            "DistributorName",
            "Region",
            "Province",
            "City",
            "DistributorType",
            "StartYear",
        ],
        "raw_columns": [
            "distributor_id",
            "distributor_name",
            "region",
            "province",
            "city",
            "distributor_type",
            "start_year",
        ],
    },

    "DimSalesperson": {
        "table": "raw.salesperson",
        "source_columns": [
            "SalespersonID",
            "SalespersonName",
            "Supervisor",
            "DistributorID",
            "Territory",
            "JoinYear",
        ],
        "raw_columns": [
            "salesperson_id",
            "salesperson_name",
            "supervisor",
            "distributor_id",
            "territory",
            "join_year",
        ],
    },

    "DimOutlet": {
        "table": "raw.outlet",
        "source_columns": [
            "OutletID",
            "OutletName",
            "Channel",
            "OutletTier",
            "DistributorID",
            "SalespersonID",
            "City",
            "Province",
            "Region",
            "OpenYear",
        ],
        "raw_columns": [
            "outlet_id",
            "outlet_name",
            "channel",
            "outlet_tier",
            "distributor_id",
            "salesperson_id",
            "city",
            "province",
            "region",
            "open_year",
        ],
    },

    "FactTarget": {
        "table": "raw.target",
        "source_columns": [
            "MonthStart",
            "SalespersonID",
            "TargetSales",
            "TargetActiveOutlets",
        ],
        "raw_columns": [
            "month_start",
            "salesperson_id",
            "target_sales",
            "target_active_outlets",
        ],
    },

    "FactInventory": {
        "table": "raw.inventory",
        "source_columns": [
            "MonthStart",
            "DistributorID",
            "ProductID",
            "OpeningStock",
            "StockReceived",
            "UnitsSold",
            "ClosingStock",
            "StockValue",
        ],
        "raw_columns": [
            "month_start",
            "distributor_id",
            "product_id",
            "opening_stock",
            "stock_received",
            "units_sold",
            "closing_stock",
            "stock_value",
        ],
    },
}


PIPELINE_NAME = "master_data_ingestion"


def start_pipeline_run(conn, source_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.pipeline_runs (
                pipeline_name,
                source_name,
                status
            )
            VALUES (%s, %s, 'RUNNING')
            RETURNING run_id;
            """,
            (
                PIPELINE_NAME,
                source_name,
            ),
        )

        return cur.fetchone()[0]


def is_already_processed(conn, source_name):
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
                PIPELINE_NAME,
                source_name,
            ),
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


def validate_columns(
    received_columns,
    expected_columns,
):
    received_columns = list(received_columns)

    missing = [
        column
        for column in expected_columns
        if column not in received_columns
    ]

    unexpected = [
        column
        for column in received_columns
        if column not in expected_columns
    ]

    if missing or unexpected:
        raise ValueError(
            f"""
Schema validation failed.

Missing:
{missing}

Unexpected:
{unexpected}

Expected:
{expected_columns}

Received:
{received_columns}
"""
        )


def to_raw_value(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    return str(value)


def ingest_sheet(sheet_name):
    config = SHEET_CONFIG[sheet_name]

    source_name = (
        f"{MASTER_FILE.name}:{sheet_name}"
    )

    print("=" * 70)
    print("MASTER DATA INGESTION")
    print("=" * 70)
    print(f"Sheet  : {sheet_name}")

    with get_connection() as conn:

        if is_already_processed(
            conn,
            source_name,
        ):
            print(
                "Status : SKIPPED "
                "(already processed)"
            )
            return

        run_id = start_pipeline_run(
            conn,
            source_name,
        )

        conn.commit()

        print(f"Run ID : {run_id}")

        try:
            df = pd.read_excel(
                MASTER_FILE,
                sheet_name=sheet_name,
            )

            df = df.dropna(how="all")

            df.columns = [
                str(column).strip()
                for column in df.columns
            ]

            validate_columns(
                df.columns,
                config["source_columns"],
            )

            raw_columns = (
                config["raw_columns"]
                + [
                    "source_file",
                    "source_sheet",
                    "source_row_number",
                    "pipeline_run_id",
                ]
            )

            copy_sql = f"""
                COPY {config["table"]} (
                    {", ".join(raw_columns)}
                )
                FROM STDIN
            """

            records_received = 0

            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:

                    for row_number, (_, row) in enumerate(
                        df.iterrows(),
                        start=2,
                    ):
                        values = [
                            to_raw_value(
                                row[column]
                            )
                            for column
                            in config["source_columns"]
                        ]

                        copy.write_row(
                            (
                                *values,
                                MASTER_FILE.name,
                                sheet_name,
                                row_number,
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

            print(
                f"Rows   : "
                f"{records_received:,}"
            )
            print("Status : SUCCESS")

        except Exception as exc:
            conn.rollback()

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
    for sheet_name in SHEET_CONFIG:
        ingest_sheet(sheet_name)