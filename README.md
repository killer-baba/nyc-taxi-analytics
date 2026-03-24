# NYC Taxi Analytics — Data Engineering Platform

## Overview

A production-grade data engineering pipeline built on the **NYC TLC Yellow Taxi Trip Records (2023)** dataset (~38.3 million rows, ~3-4 GB). This project demonstrates end-to-end data platform capabilities spanning ingestion, transformation, orchestration, analytical SQL, and large-scale distributed processing.

**Built for**: Firmable — Data Engineer (Platform) Technical Assessment

---

## Tech Stack

| Component       | Technology                          | Rationale                                                                 |
|-----------------|--------------------------------------|---------------------------------------------------------------------------|
| Warehouse       | Snowflake (X-Small)                 | Production-grade, assessment-preferred dialect, separation of storage and compute |
| Transformation  | DBT Core 1.11 + Snowflake adapter   | Industry-standard transformation layer, declarative SQL modelling         |
| Orchestration   | Apache Airflow                       | DAG-based scheduling with retry logic, backfill support, and alerting     |
| Batch Processing| PySpark                              | Distributed processing for historical scale (1.5B+ rows)                 |
| Language        | SQL (Snowflake), Python 3.11        |                                                                           |

---

## Architecture

### High-Level Data Flow

```
  ┌──────────────────┐
  │   NYC TLC Open   │        12 monthly Parquet files
  │   Data (S3/HTTPS)│        (~38.3M rows, 2023)
  └────────┬─────────┘
           │
           ▼
  ┌──────────────────┐
  │  Python Loader   │        PUT + COPY INTO via snowflake-connector
  │  (one-time ETL)  │        Handles Parquet timestamp interpretation
  └────────┬─────────┘
           │
           ▼
  ┌──────────────────────────────────────────────────────┐
  │                 Snowflake (NYC_TAXI_DB)               │
  │                                                       │
  │  ┌─────────┐   ┌──────────────┐   ┌──────────────┐  │
  │  │   RAW   │──▶│   STAGING    │──▶│ INTERMEDIATE │  │
  │  │         │   │              │   │              │  │
  │  │ yellow_ │   │ stg_yellow_  │   │ int_trips_   │  │
  │  │tripdata │   │ trips        │   │ enriched     │  │
  │  │         │   │ stg_taxi_    │   │              │  │
  │  │         │   │ zones        │   │ (joins +     │  │
  │  │(source) │   │ (views)      │   │  filters)    │  │
  │  └─────────┘   └──────────────┘   └──────┬───────┘  │
  │                                           │          │
  │                                           ▼          │
  │                                   ┌──────────────┐   │
  │                                   │    MARTS     │   │
  │                                   │              │   │
  │                                   │ fct_trips    │   │
  │                                   │ dim_zones    │   │
  │                                   │ agg_daily_   │   │
  │                                   │  revenue     │   │
  │                                   │ agg_zone_    │   │
  │                                   │  performance │   │
  │                                   │ (tables)     │   │
  │                                   └──────────────┘   │
  └──────────────────────────────────────────────────────┘
           ▲
           │  Orchestrated by
  ┌────────┴─────────┐
  │   Airflow DAG    │   Daily at 02:00 UTC
  │                  │   source check → staging → intermediate
  │  nyc_taxi_daily  │   → marts → tests → notify
  │  _pipeline       │   retries=2, backfill-safe
  └──────────────────┘
```

### Dimensional Model

```
                    ┌──────────────────┐
                    │    dim_zones     │
                    │                  │
                    │ location_id (PK) │
                    │ borough          │
                    │ zone_name        │
                    │ service_zone     │
                    └────────┬─────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │ pickup_location  │ dropoff_location  │
          │                  │                   │
  ┌───────┴──────────────────┴───────────────┐
  │              fct_trips                    │
  │                                          │
  │  trip_id (PK)          fare_amount       │
  │  pickup_datetime       tip_amount        │
  │  dropoff_datetime      total_amount      │
  │  pickup_location_id    payment_type      │
  │  dropoff_location_id   trip_duration_min │
  │  passenger_count       pickup_zone_name  │
  │  trip_distance         dropoff_zone_name │
  └──────────────────────────────────────────┘
          │                         │
          ▼                         ▼
  ┌─────────────────┐   ┌─────────────────────┐
  │agg_daily_revenue│   │agg_zone_performance │
  │                 │   │                     │
  │ trip_date       │   │ pickup_zone         │
  │ total_trips     │   │ month               │
  │ total_fare      │   │ total_trips         │
  │ avg_fare        │   │ avg_trip_distance   │
  │ total_tips      │   │ avg_fare            │
  │ tip_rate_pct    │   │ revenue_rank        │
  └─────────────────┘   │ high_volume_flag    │
                        └─────────────────────┘
```

---

## Repository Structure

```
├── dbt/                        # DBT project
│   ├── models/
│   │   ├── staging/            # Raw → clean (views)
│   │   ├── intermediate/       # Joins + filtering (views)
│   │   └── marts/              # Business-facing tables
│   ├── tests/                  # Custom singular + generic tests
│   ├── seeds/                  # Zone lookup CSV (265 rows)
│   ├── macros/                 # Custom schema routing macro
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── README.md               # 📖 Detailed DBT pipeline documentation
├── dags/                       # Airflow DAG
│   ├── nyc_taxi_daily_pipeline.py
│   └── README.md               # 📖 DAG architecture + blue/green brainstormer
├── queries/                    # Standalone analytical SQL
│   ├── q1_top_zones_by_revenue.sql
│   ├── q2_hour_of_day_pattern.sql
│   ├── q3_consecutive_gap_analysis.sql
│   └── README.md               # 📖 Query explanations + performance strategies
├── spark/                      # PySpark historical processing (bonus)
│   ├── process_historical.py
│   └── README.md               # 📖 Spark optimizations + EMR/Glue deployment
├── setup_scripts/              # One-time environment setup
│   ├── 01_snowflake_setup.sql
│   ├── 02_load_to_snowflake.py
│   ├── load_data.py
│   └── README.md               # 📖 Setup script execution order
├── requirements.txt
├── .gitignore
├── EXECUTION_GUIDE.md          # 📖 Step-by-step setup and run instructions
└── README.md                   # ← You are here
```

### Internal Documentation Guide

Each component has its own detailed README. Start here for the overview, then dive into specifics:

| Document | What You'll Find |
|---|---|
| [`dbt/README.md`](dbt/README.md) | Full pipeline flow with row counts, schema changes table, surrogate key design, deduplication strategy, all 30 tests listed, brainstormer answer on revenue ranking |
| [`dags/README.md`](dags/README.md) | Task flow diagram, operator choices, backfill handling, retry strategy, **blue/green deployment brainstormer** |
| [`queries/README.md`](queries/README.md) | Each query explained with approach, expected output, performance strategies, verified runtimes |
| [`spark/README.md`](spark/README.md) | Processing pipeline, optimization decisions table, EMR vs Glue comparison, deployment instructions |
| [`setup_scripts/README.md`](setup_scripts/README.md) | Execution order for environment initialization, troubleshooting notes |
| [`EXECUTION_GUIDE.md`](EXECUTION_GUIDE.md) | Step-by-step instructions to reproduce the entire project from scratch |

---

## Modelling Decisions

### Why Snowflake Over DuckDB

The assessment offers DuckDB as an alternative, but Snowflake was chosen because:
- The SQL queries (Task 3) target Snowflake dialect and reference Snowflake-specific features (clustering keys, search optimization, result caching)
- Snowflake's separation of storage and compute is a key architectural concept worth demonstrating
- Production data engineering roles operate on cloud warehouses, not embedded databases
- The free trial with an X-Small warehouse handles 38M rows comfortably within the assessment timeline

### Materialization Strategy

| Layer        | Materialization | Why |
|--------------|-----------------|-----|
| Staging      | View            | Zero storage cost. Always reflects current source. No direct consumers. |
| Intermediate | View            | Transient transform. Persisting adds storage cost with no query benefit. |
| Marts        | Table           | Pre-computed for analyst performance. Acceptable staleness in daily batch. |

**Why not incremental for fct_trips?** Incremental materialisation processes only new rows since the last run — ideal for production fact tables receiving daily appends. However, this dataset is a one-time historical load of a fixed year (2023). There are no daily appends. Table materialisation is the correct choice here. In a production pipeline with ongoing data ingestion, fct_trips would be incremental.

### Zone Data as DBT Seed

The taxi zone lookup (~265 rows) is loaded as a DBT seed rather than an external source. This is the correct pattern when reference data is small, rarely changes, and benefits from version control. Any change to zone definitions is tracked in Git alongside the models that use it.

---

## Data Quality Strategy

**30 tests — all passing.** Full test inventory documented in [`dbt/README.md`](dbt/README.md).

| Category | Count | Examples |
|---|---|---|
| Primary key (not_null + unique) | 12 | trip_id, location_id, trip_date |
| Not null on critical fields | 10 | timestamps, location IDs, borough |
| Referential integrity | 2 | fct_trips.pickup/dropoff_location_id → dim_zones |
| Accepted values | 1 | payment_type (0-5) |
| Custom singular test | 1 | total_amount ≥ fare_amount (business rule) |
| Custom generic test | 1 | trip_duration_minutes in range [1, 180] |
| **Total** | **30** | **All passing** |

---

## SQL Query Performance

All queries verified on Snowflake X-Small warehouse. Full analysis in [`queries/README.md`](queries/README.md).

| Query | Purpose | Runtime | Rows |
|---|---|---|---|
| Q1 | Top 10 zones by revenue per month | **2.4s** | 120 |
| Q2 | Hour-of-day demand pattern | **722ms** | 24 |
| Q3 | Consecutive trip gap analysis | **3.0s** | 365 |

---

## Brainstormer Responses

### 1. Revenue Rank: Monthly vs. Overall (Task 1)

**Decision**: Rank within each month — `RANK() OVER (PARTITION BY trip_month ORDER BY total_revenue DESC)`.

An overall rank produces a single static number that hides seasonal patterns. Times Square might rank #1 overall, but that masks the fact that it drops significantly in February and surges in December. Monthly ranking reveals seasonal trends, enables month-over-month performance tracking, supports monthly resource allocation decisions (where to stage taxis), and surfaces anomalies (a zone dropping 20 rank positions signals investigation).

Full justification with SQL implementation in [`dbt/README.md`](dbt/README.md) and in the model SQL file itself.

### 2. Blue/Green Deployment for Data Quality (Task 2)

**Problem**: If `run_dbt_tests` fails after `run_dbt_marts` has written to production, downstream dashboards read bad data.

**Proposed solution: Schema-swap pattern.**

1. DBT writes mart tables to a `MARTS_STAGING` schema (not production `MARTS`)
2. All 30 tests validate against `MARTS_STAGING`
3. If tests pass: swap schemas atomically via `ALTER SCHEMA RENAME` (metadata-only, instant in Snowflake)
4. If tests fail: drop `MARTS_STAGING`, production `MARTS` is untouched, alert fires

This is the production-grade approach used at companies running dbt at scale. The schema swap is atomic — no window where consumers see partial or bad data. Trade-offs: adds complexity (extra schema, swap logic) and requires temporary double storage during the build phase.

Full implementation discussion with Airflow task modifications in [`dags/README.md`](dags/README.md).

### 3. Query 3 Performance at Scale (Task 3)

The consecutive gap analysis requires `LAG()` across 38M rows, which demands sorting by (zone, date, pickup_time). Snowflake-specific optimizations:

- **Clustering key**: `CLUSTER BY (pulocationid, tpep_pickup_datetime::DATE)` physically aligns data with the query's PARTITION BY pattern — estimated 4-8x speedup
- **Search optimization**: helps filter specific zones or date ranges
- **Materialized view**: pre-computes LAG values, maintained incrementally by Snowflake
- **Result caching**: identical queries return cached results in <1s for 24 hours
- **Warehouse sizing**: temporarily scale to MEDIUM for one-time heavy computation

Full performance analysis with cost/benefit per strategy in [`queries/README.md`](queries/README.md).

---

## Trade-offs and Shortcuts

| Decision | Trade-off | Why It's Acceptable |
|---|---|---|
| Table materialization for fct_trips | Not incremental — full rebuild on each run | Dataset is static 2023 data, no daily appends. Incremental would be correct for production with ongoing ingestion. |
| BashOperator for dbt in Airflow | Less error granularity than Cosmos/dbt Cloud operator | Simpler, no extra dependencies, widely adopted for dbt Core. Sufficient for 6-task linear DAG. |
| `generate_surrogate_key` via MD5 hash | Hash collision theoretically possible | 10-field hash on 38M rows yielded 1,804 collisions (true source duplicates). Deduplicated via ROW_NUMBER. |
| Parquet timestamp fix (`USE_LOGICAL_TYPE`) | Requires awareness of Snowflake-Parquet interaction | Documented in setup scripts and troubleshooting. Common real-world issue. |
| Spark script not executed on cluster | Cannot verify runtime at 1.5B scale | Script is logically correct and follows Spark best practices. Assessment states execution is not required. |
| `catchup=False` in Airflow | No automatic historical backfill | Prevents accidental mass backfill on deployment. Manual backfill available via CLI. |

---

## AI Tools Usage

This project was developed with **Claude (Anthropic)** as a technical mentor throughout the entire development process. The approach was deliberately step-by-step: understand the concept, discuss trade-offs, then implement.

**How AI was used:**
- **Architecture planning**: Discussed materialization strategies, layer design, dimensional modelling, and Snowflake schema design before writing any code
- **Environment setup**: Guided through venv creation, dbt init, Snowflake configuration, and data loading — every command explained before execution
- **Debugging**: Diagnosed Parquet timestamp loading issue (`USE_LOGICAL_TYPE`), surrogate key collisions (added deduplication), dbt 1.11 deprecation syntax changes, and Snowflake schema naming (`generate_schema_name` macro)
- **Code generation**: AI provided code that was reviewed, understood, and tested before inclusion. Every file was verified against Snowflake before moving on.
- **Documentation**: READMEs written incrementally during development, not retroactively. Each component's README was created alongside the code.

**Productivity impact**: AI mentoring allowed for a more thorough and well-documented project than would have been possible in the same timeframe working solo. The primary speedup came from having immediate access to debugging knowledge (e.g., Parquet timestamp issues, dbt schema naming) and architectural patterns (e.g., blue/green deployment, Spark broadcast joins) without needing to search documentation.