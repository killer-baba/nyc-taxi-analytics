# PySpark — Historical Data Processing

## What This Script Does

Processes the **full NYC TLC Yellow Taxi dataset from 2009 to 2023** (~1.5 billion rows). Applies the same cleaning and filtering logic as the DBT pipeline, computes daily revenue aggregation, and writes output as partitioned Parquet files.

This is the "scale beyond a single warehouse" solution — when data is too large for direct loading, Spark handles distributed processing across a cluster.

---

## Processing Pipeline

```
Input: 150+ Parquet files from S3 or local disk
  (~1.5 billion rows, 2009-2023)
        │
        ▼
┌─────────────────────────────────────────────────┐
│  READ                                           │
│  - Explicit schema (avoids slow inference)       │
│  - mergeSchema for cross-year schema evolution   │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│  CLEAN (mirrors DBT staging + intermediate)     │
│  - Rename columns to snake_case                  │
│  - Cast passenger_count, payment_type to INT     │
│  - Compute trip_duration_minutes                 │
│  - Filter: distance > 0, fare > 0               │
│  - Filter: passenger_count > 0                   │
│  - Filter: 1 min ≤ duration ≤ 180 min           │
│  - Filter: year between 2009 and 2023            │
│  - cache() — DataFrame reused for count + agg   │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│  AGGREGATE (mirrors DBT agg_daily_revenue)      │
│  - Group by: trip_date, year, month              │
│  - Metrics: total_trips, total_fare, avg_fare,   │
│    total_tips, tip_rate_pct                      │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│  WRITE                                          │
│  - repartition by (year, month)                  │
│  - Write as Parquet, partitioned by year/month   │
│  - Output structure:                             │
│    output/year=2023/month=1/part-00000.parquet   │
│    output/year=2023/month=2/part-00000.parquet   │
└─────────────────────────────────────────────────┘
```

---

## Optimization Decisions

| Optimization | Where | Why |
|---|---|---|
| **Explicit schema** | `read_trip_data()` | Avoids scanning all 150+ file footers for schema inference. Also handles schema evolution (older years have fewer columns). |
| **`cache()`** | After `clean_trips()` | Cleaned DataFrame is used twice (count + aggregation). Without cache, Spark re-reads and re-processes all Parquet files for each action. |
| **`broadcast` join** | Spark config | `autoBroadcastJoinThreshold` set to 100MB. The zone lookup (265 rows, ~10KB) is automatically broadcast to all nodes, avoiding a shuffle of the 1.5B row trip table. |
| **`repartition`** | Before `write` | Aligns Spark partitions with output partitions (year/month). Without this, a single month's data might be fragmented across dozens of tiny files. |
| **Adaptive Query Execution** | Spark config | `spark.sql.adaptive.enabled=true` lets Spark auto-optimize shuffle partition count and join strategies at runtime based on actual data sizes. |
| **Parquet filter pushdown** | Spark config | `spark.sql.parquet.filterPushdown=true` pushes WHERE clauses into the Parquet reader, skipping entire row groups that don't match filters. |

---

## Usage

### Local Testing (with 2023 data subset)

```bash
spark-submit spark/process_historical.py \
    --input data/ \
    --output output/daily_revenue
```

### AWS EMR (full 2009-2023 dataset)

```bash
# Submit to EMR cluster
spark-submit spark/process_historical.py \
    --input "s3://nyc-tlc/trip data/yellow_tripdata_*.parquet" \
    --output s3://my-bucket/nyc-taxi/daily_revenue
```

### AWS Glue

Create a Glue ETL job pointing to `process_historical.py` with:
- Worker type: G.1X
- Number of workers: 20
- Job parameters: `--input`, `--output` as above

---

## Deployment: EMR vs Glue

| Factor | EMR | Glue |
|---|---|---|
| Control | Full Spark config control | Limited to Glue parameters |
| Setup | Must manage cluster lifecycle | Serverless — no cluster management |
| Cost | Pay per instance-hour | Pay per DPU-hour |
| Best for | Recurring heavy workloads, custom tuning | One-time jobs, simpler pipelines |
| This use case | Better — one-time historical backfill benefits from cluster sizing control | Acceptable but less control over parallelism |

**Recommendation for this use case**: EMR with spot instances. A one-time historical backfill of 1.5B rows benefits from a right-sized cluster (5 × m5.xlarge nodes, ~$2-4 total). After processing, terminate the cluster. Spot instances reduce cost by 60-70%.

---

## Output Schema

The partitioned Parquet output contains:

| Column | Type | Description |
|---|---|---|
| trip_date | DATE | Calendar date |
| total_trips | LONG | Number of valid trips |
| total_fare | DOUBLE | Sum of fare_amount |
| avg_fare | DOUBLE | Average fare per trip |
| total_tips | DOUBLE | Sum of tip_amount |
| tip_rate_pct | DOUBLE | Tips as % of total fare |

Partitioned by: `year` (INT), `month` (INT)

This output can be:
- Loaded into Snowflake via external tables or COPY INTO
- Queried directly by Athena/Presto as an external table
- Used as input to further Spark or dbt processing