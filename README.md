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

### DBT Layer Architecture

The project follows a **three-layer medallion pattern** — the industry-standard approach for organising transformations in a data warehouse:

**Staging (views)** — The contract layer between raw data and the rest of the pipeline. Responsible for column renaming (PascalCase → snake_case), type casting, and computed columns. No business logic lives here. Materialised as views because no one queries staging directly — it exists purely as a clean interface over raw data, with zero storage cost.

**Intermediate (views)** — The business logic layer. Joins trips with zone reference data (resolving location IDs to human-readable zone names) and enforces data quality filters (removing trips with zero distance, zero fare, impossible durations, etc.). Materialised as views because this is a transient transformation — it feeds the marts but has no direct consumers.

**Marts (tables)** — The consumption layer. This is what analysts, dashboards, and downstream systems query. Contains a fact table (fct_trips — one row per valid trip), a dimension table (dim_zones — zone reference data), and two pre-computed aggregation tables for common analytical patterns. Materialised as tables for query performance — the data is pre-computed and ready to read.

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

This follows **Kimball dimensional modelling**: facts contain measurable events (trips), dimensions contain descriptive attributes (zones). The separation allows analysts to slice facts by any dimension without data duplication. Aggregation tables sit downstream as pre-computed summaries for common queries.

---

## Repository Structure

```
├── dbt/                        # DBT project
│   ├── models/
│   │   ├── staging/            # Raw → clean (views)
│   │   ├── intermediate/       # Joins + filtering (views)
│   │   └── marts/              # Business-facing tables
│   ├── tests/                  # Custom singular tests
│   │   └── generic/            # Custom generic (parametrised) tests
│   ├── seeds/                  # Zone lookup CSV (265 rows)
│   ├── macros/                 # Reusable SQL macros
│   ├── dbt_project.yml         # Project config + materialization strategy
│   └── profiles.yml.example    # Snowflake connection template
├── dags/                       # Airflow DAG definition
│   └── nyc_taxi_daily_pipeline.py
├── queries/                    # Standalone analytical SQL
│   ├── q1_top_zones_by_revenue.sql
│   ├── q2_hour_of_day_pattern.sql
│   └── q3_consecutive_gap_analysis.sql
├── spark/                      # PySpark historical processing (bonus)
│   └── process_historical.py
├── setup_scripts/              # One-time environment setup
├── requirements.txt
├── .gitignore
├── EXECUTION_GUIDE.md          # Step-by-step setup and run instructions
└── README.md                   # ← You are here
```

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

### Built-in Tests
- `not_null` and `unique` on all primary keys
- `accepted_values` on categorical columns (payment_type)
- `relationships` tests to verify foreign key integrity between facts and dimensions

### Custom Singular Test
**`assert_total_gte_fare`**: Verifies that no trip in `fct_trips` has `total_amount < fare_amount`. Business rule: total includes fare + tips + surcharges, so it must always be >= fare. A violation indicates data corruption or incorrect loading.

### Custom Generic Test
**`test_value_in_range`**: A parametrised test that asserts a column's values fall within a configurable min/max range. Applied to `trip_duration_minutes` to catch impossible values that survive the intermediate layer filters. Reusable across any numeric column.

---

## Brainstormer Responses

*Will be added as each task is completed.*

---

## Trade-offs and Shortcuts

*Will be documented as they arise during development.*

---

## AI Tools Usage

This project was developed with **Claude (Anthropic)** as a technical mentor. The development approach was deliberately step-by-step: understand the concept, discuss trade-offs, then implement.

**How AI was used:**
- **Architecture planning**: Discussed materialization strategies, layer design, and dimensional modelling approach before writing code
- **Debugging**: Diagnosed Parquet timestamp loading issue (microsecond interpretation via `USE_LOGICAL_TYPE`) collaboratively
- **Code generation**: AI provided code that was reviewed, understood, and modified before inclusion
- **Documentation**: README and setup guides written incrementally during development, not retroactively

**What AI did NOT do:**
- Make architectural decisions unilaterally — all trade-offs were presented and I chose the approach
- Write code that I couldn't explain in an interview
- Replace understanding of the underlying concepts