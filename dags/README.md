# Airflow DAG — NYC Taxi Daily Pipeline

## What This DAG Does

Orchestrates the full DBT pipeline on a daily schedule. Checks source freshness, builds models layer by layer, runs data quality tests, and sends a success notification. If any step fails, downstream tasks are skipped and an email alert fires.

---

## DAG Overview

```
DAG ID:     nyc_taxi_daily_pipeline
Schedule:   Daily at 02:00 UTC (0 2 * * *)
Start Date: 2023-01-01
Catchup:    False (prevents mass backfill on deploy)
Max Active: 1 (prevents concurrent runs)
Retries:    2 per task, 5 min delay between retries
Alerts:     email_on_failure = True
```

---

## Task Flow

```
check_source_freshness      Verify upstream data is fresh (dbt source freshness)
        │                   Fails DAG early if source data is stale/missing
        ▼
run_dbt_staging             Build staging views (stg_yellow_trips, stg_taxi_zones)
        │                   Rename columns, cast types, compute duration, dedup
        ▼
run_dbt_intermediate        Build intermediate view (int_trips_enriched)
        │                   Join zones, filter invalid records
        ▼
run_dbt_marts               Build mart tables (fct_trips, dim_zones,
        │                   agg_daily_revenue, agg_zone_performance)
        ▼
run_dbt_tests               Run all 30 dbt tests
        │                   FAILS THE DAG if any test fails
        │                   Prevents bad data from being consumed
        ▼
notify_success              Log success summary with execution date
                            In production: Slack message or monitoring table
```

**If any task fails**: All downstream tasks are skipped. Airflow's default `trigger_rule` is `all_success`, meaning a task only runs if all its upstream tasks succeeded.

---

## Task Implementation

| Task | Operator | What It Runs |
|---|---|---|
| check_source_freshness | PythonOperator | `dbt source freshness` — checks MAX(pickup_datetime) against threshold |
| run_dbt_staging | BashOperator | `dbt run --select staging` |
| run_dbt_intermediate | BashOperator | `dbt run --select intermediate` |
| run_dbt_marts | BashOperator | `dbt run --select marts` |
| run_dbt_tests | BashOperator | `dbt test` — non-zero exit code = task failure |
| notify_success | PythonOperator | Logs summary via Python logging module |

**Why BashOperator for dbt?** It's the simplest, most widely adopted pattern for running dbt Core from Airflow. No extra library dependencies. The dbt CLI returns non-zero exit codes on failure, which BashOperator translates directly into task failures. Alternatives like astronomer-cosmos or dbt Cloud operators add complexity without benefit for this use case.

---

## Key Design Decisions

### No Hardcoded Credentials

- Snowflake credentials are read from environment variables by dbt (via `profiles.yml` using `env_var()`)
- The `DBT_PROJECT_DIR` path is configurable via environment variable
- Alert email is configurable via `ALERT_EMAIL` environment variable
- No secrets appear anywhere in the DAG file

### Backfill Support

The DAG handles backfill correctly:

- `execution_date` (logical_date) is passed to all tasks via Airflow's context
- Our dbt models process the full 2023 dataset (not incremental), so any run produces the same result regardless of execution date — inherently idempotent
- `catchup=False` prevents accidental mass backfill on deployment
- `max_active_runs=1` prevents concurrent runs from conflicting
- Manual backfill: `airflow dags backfill nyc_taxi_daily_pipeline --start-date 2023-06-01 --end-date 2023-06-30`

### Retry Strategy

```
retries=2, retry_delay=5 minutes
```

Transient failures (Snowflake warehouse waking up, network blips, temporary lock conflicts) are handled automatically. If a task fails 3 times (1 attempt + 2 retries), it's a real failure and the email alert fires.

### Why Tests Run After Marts (Not Before)

Tests validate the mart tables. They must exist before they can be tested. The sequence is: build marts → test marts → if tests fail, alert.

The risk: if tests fail, the mart tables already contain potentially bad data that downstream consumers might read. See the brainstormer below for how to solve this in production.

---

## Brainstormer: Blue/Green Deployment for Data Quality

**Problem**: If `run_dbt_tests` fails after `run_dbt_marts` has already written to the production `MARTS` schema, downstream dashboards are reading bad data until the issue is fixed.

**Proposed Solution: Schema-swap pattern**

```
1. dbt run writes to a STAGING copy: MARTS_STAGING schema
2. dbt test validates MARTS_STAGING tables
3. IF tests pass:
     - Swap schemas: ALTER SCHEMA MARTS RENAME TO MARTS_OLD;
                     ALTER SCHEMA MARTS_STAGING RENAME TO MARTS;
                     DROP SCHEMA MARTS_OLD;
4. IF tests fail:
     - MARTS_STAGING is dropped
     - Production MARTS schema is untouched
     - Alert fires
```

**How to implement in dbt + Airflow:**

- Use dbt's `--target` flag to point to a separate profile output that writes to `MARTS_STAGING`
- Add a new Airflow task `swap_schemas` between `run_dbt_tests` and `notify_success`
- The swap task uses `SnowflakeOperator` or `PythonOperator` to execute the ALTER SCHEMA commands
- Schema swaps in Snowflake are metadata-only operations — instant, regardless of table size

**Trade-offs:**

- Adds complexity (extra schema, swap logic, rollback handling)
- Requires temporary double storage during the build phase
- Schema swap is atomic — no window where consumers see partial data

This is the production-grade approach used at companies running dbt at scale. For this assessment, the simpler "test after build" pattern is implemented, with this discussion documenting the awareness of the limitation.

---

## File Structure

```
dags/
└── nyc_taxi_daily_pipeline.py    # Single DAG file — all tasks defined here
```

In production, you might split helpers into a separate `dags/utils/` module, but for a single DAG with 6 tasks, one file is cleaner and easier to review.