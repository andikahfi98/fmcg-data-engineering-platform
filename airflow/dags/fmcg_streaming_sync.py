from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="fmcg_streaming_sync",
    description=(
        "Micro-batch transformation of Kafka sales events "
        "from PostgreSQL RAW into the warehouse"
    ),

    default_args=default_args,

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="Asia/Jakarta",
    ),

    # Every 5 minutes
    schedule="*/5 * * * *",

    catchup=False,
    max_active_runs=1,

    dagrun_timeout=timedelta(
        minutes=10
    ),

    tags=[
        "fmcg",
        "streaming",
        "kafka",
        "dbt",
    ],
) as dag:

    dbt_streaming_build = BashOperator(
        task_id="dbt_streaming_build",

        bash_command="""
        cd /opt/project &&
        dbt build \
          --project-dir dbt \
          --profiles-dir dbt \
          --select \
            stg_sales_streaming \
            stg_sales_unified \
            fact_sales
        """,

        execution_timeout=timedelta(
            minutes=5
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
            minutes=5
        ),
    )


    pipeline_complete = BashOperator(
        task_id="streaming_sync_complete",

        bash_command="""
        echo "FMCG STREAMING WAREHOUSE SYNC COMPLETED"
        """,
    )


    (
        dbt_streaming_build
        >> validate_pipeline
        >> pipeline_complete
    )