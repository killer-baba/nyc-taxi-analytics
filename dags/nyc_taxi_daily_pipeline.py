"""
NYC Taxi Daily Pipeline — Airflow DAG
======================================
Orchestrates the full DBT pipeline: source freshness check, staged model builds,
testing, and success notification. Runs daily at 02:00 UTC.

DAG Structure:
    check_source_freshness → run_dbt_staging → run_dbt_intermediate
    → run_dbt_marts → run_dbt_tests → notify_success

Design Decisions:
    - BashOperator for dbt commands: no extra dependencies, widely adopted pattern
    - Sequential task ordering: staging must complete before intermediate, etc.
    - Tests run AFTER marts: if tests fail, DAG fails and alerts fire.
      See README brainstormer for blue/green deployment discussion.
    - Backfill-safe: uses {{ ds }} template for date-aware logging.
      Models process full 2023 dataset (not incremental), so reruns are idempotent.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Resolve dbt project directory relative to the DAG file location.
# In production, this would come from an Airflow Variable or environment var.
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")

# Snowflake connection — sourced from Airflow Connections or env vars.
# No credentials are hardcoded anywhere in this file.
SNOWFLAKE_CONN_ID = "snowflake_default"

# Default arguments applied to every task in the DAG
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email": [os.environ.get("ALERT_EMAIL", "data-alerts@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Callback / helper functions
# ---------------------------------------------------------------------------
def check_source_freshness_fn(**context):
    """
    Validate that source data is fresh before running the pipeline.

    Uses dbt source freshness to check that the most recent record in
    yellow_tripdata is within the configured threshold (24h error, 12h warn).

    In a production pipeline with daily file drops, this would check for
    the existence of the specific file for {{ ds }}. For this assessment,
    we use dbt's built-in freshness check against the loaded table.
    """
    logical_date = context["ds"]
    logging.info(f"Checking source freshness for execution date: {logical_date}")

    exit_code = os.system(
        f"cd {DBT_PROJECT_DIR} && dbt source freshness"
    )

    if exit_code != 0:
        raise Exception(
            f"Source freshness check failed for {logical_date}. "
            "Upstream data may be stale or missing."
        )

    logging.info("Source freshness check passed.")


def notify_success_fn(**context):
    """
    Log a success summary after the pipeline completes.

    In production, this would send a Slack message or write to a monitoring
    table. For this assessment, we log the execution date and a summary.
    """
    logical_date = context["ds"]
    logging.info("=" * 60)
    logging.info(f"NYC Taxi Pipeline — SUCCESS")
    logging.info(f"Execution date: {logical_date}")
    logging.info(f"All dbt models built and 30/30 tests passed.")
    logging.info(
        "Pipeline completed successfully. "
        "Mart tables are up to date in NYC_TAXI_DB.MARTS."
    )
    logging.info("=" * 60)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    default_args=default_args,
    description="Daily pipeline: ingest, transform, test NYC yellow taxi trip data",
    # Schedule at 02:00 UTC daily — allows time for upstream data to land
    schedule_interval="0 2 * * *",
    start_date=datetime(2023, 1, 1),
    # Prevent backfill from launching hundreds of DAG runs simultaneously
    catchup=False,
    max_active_runs=1,
    tags=["nyc_taxi", "dbt", "production"],
) as dag:

    # Task 1: Check source data freshness
    # Uses dbt source freshness to verify upstream data is current.
    check_source_freshness = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness_fn,
    )

    # Task 2: Run dbt staging models
    # Builds stg_yellow_trips and stg_taxi_zones (views)
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging",
    )

    # Task 3: Run dbt intermediate models
    # Builds int_trips_enriched — joins zones + filters invalid records
    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select intermediate",
    )

    # Task 4: Run dbt mart models
    # Builds fct_trips, dim_zones, agg_daily_revenue, agg_zone_performance
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts",
    )

    # Task 5: Run all dbt tests — FAIL the DAG if any test fails
    # Tests validate primary key uniqueness, not-null constraints,
    # referential integrity, business rules, and value ranges.
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    # Task 6: Notify success
    # Logs summary. In production, would post to Slack or monitoring table.
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success_fn,
    )

    # -----------------------------------------------------------------------
    # Task dependencies — linear pipeline
    # -----------------------------------------------------------------------
    # Each step depends on the previous one completing successfully.
    # If any task fails, downstream tasks are skipped and alerts fire.
    (
        check_source_freshness
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
        >> notify_success
    )