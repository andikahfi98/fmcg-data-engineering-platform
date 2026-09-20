from pathlib import Path

from src.utils.database import get_connection
from src.utils.file_metadata import get_file_metadata


BASE_DIR = Path(__file__).resolve().parents[2]

FACT_SALES_DIR = (
    BASE_DIR
    / "data"
    / "source"
    / "fact_sales"
)

MASTER_FILE = (
    BASE_DIR
    / "data"
    / "source"
    / "KopDes_MasterData.xlsx"
)


def resolve_source_file(
    pipeline_name: str,
    source_name: str,
) -> Path:

    if pipeline_name == "fact_sales_ingestion":
        return FACT_SALES_DIR / source_name

    if pipeline_name == "master_data_ingestion":
        return MASTER_FILE

    raise ValueError(
        f"Unsupported pipeline: {pipeline_name}"
    )


def backfill_source_metadata():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    run_id,
                    pipeline_name,
                    source_name
                FROM metadata.pipeline_runs
                WHERE status = 'SUCCESS'
                  AND source_hash IS NULL
                ORDER BY run_id;
                """
            )

            runs = cur.fetchall()

        print(f"Runs to backfill: {len(runs)}")

        updated = 0

        for run_id, pipeline_name, source_name in runs:

            file_path = resolve_source_file(
                pipeline_name,
                source_name,
            )

            if not file_path.exists():
                print(
                    f"SKIPPED run_id={run_id} "
                    f"- file not found: {file_path}"
                )
                continue

            metadata = get_file_metadata(file_path)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.pipeline_runs
                    SET
                        source_hash = %s,
                        source_size_bytes = %s,
                        source_modified_at = %s
                    WHERE run_id = %s;
                    """,
                    (
                        metadata.file_hash,
                        metadata.file_size_bytes,
                        metadata.source_modified_at,
                        run_id,
                    ),
                )

            updated += 1

            print(
                f"UPDATED run_id={run_id} "
                f"| {pipeline_name} "
                f"| {source_name}"
            )

        conn.commit()

        print("-" * 70)
        print(f"Updated : {updated}")
        print("Status  : SUCCESS")


if __name__ == "__main__":
    backfill_source_metadata()