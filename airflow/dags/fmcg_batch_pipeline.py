from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


# ============================================================
# DEFAULT TASK CONFIGURATION
# ============================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="fmcg_batch_pipeline",
    description=(
        "FMCG batch ingestion, transformation, "
        "data quality, and warehouse pipeline"
    ),

    default_args=default_args,

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="Asia/Jakarta",
    ),

    # Run every day at 21:00 WIB
    schedule="0 21 * * *",

    catchup=False,

    # Prevent overlapping pipeline runs
    max_active_runs=1,

    # Maximum duration for one complete DAG run
    dagrun_timeout=timedelta(hours=1),

    tags=[
        "fmcg",
        "data-engineering",
        "batch",
        "postgres",
        "dbt",
    ],
) as dag:

    # ========================================================
    # INGEST FACT SALES
    # ========================================================

    ingest_fact_sales = BashOperator(
        task_id="ingest_fact_sales",

        bash_command="""
        cd /opt/project &&
        python -m src.ingestion.load_fact_sales
        """,

        execution_timeout=timedelta(
            minutes=15
        ),
    )


    # ========================================================
    # INGEST MASTER DATA
    # ========================================================

    ingest_master_data = BashOperator(
        task_id="ingest_master_data",

        bash_command="""
        cd /opt/project &&
        python -m src.ingestion.load_master_data
        """,

        execution_timeout=timedelta(
            minutes=15
        ),
    )


    # ========================================================
    # DBT TRANSFORMATION
    # ========================================================

    dbt_run = BashOperator(
        task_id="dbt_run",

        bash_command="""
        cd /opt/project &&
        dbt run \
          --project-dir dbt \
          --profiles-dir dbt
        """,

        execution_timeout=timedelta(
            minutes=30
        ),
    )


    # ========================================================
    # DBT DATA QUALITY
    # ========================================================

    dbt_test = BashOperator(
        task_id="dbt_test",

        bash_command="""
        cd /opt/project &&
        dbt test \
          --project-dir dbt \
          --profiles-dir dbt
        """,

        execution_timeout=timedelta(
            minutes=30
        ),
    )

    validate_pipeline = BashOperator(
    task_id="validate_pipeline",

    bash_command="""
    cd /opt/project &&
    python -m src.monitoring.validate_pipeline \
      --dag-run-id "{{ run_id }}"
    """,

    execution_timeout=timedelta(
        minutes=10
    ),
)


    # ========================================================
    # PIPELINE COMPLETE
    # ========================================================

    pipeline_complete = BashOperator(
        task_id="pipeline_complete",

        bash_command="""
        echo "=================================================="
        echo "FMCG BATCH PIPELINE COMPLETED SUCCESSFULLY"
        echo "=================================================="
        """,
    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    (
        ingest_fact_sales
        >> ingest_master_data
        >> dbt_run
        >> dbt_test
        >> validate_pipeline
        >> pipeline_complete
    )