# DBT Project — NYC Taxi Analytics

## What This Pipeline Does

Takes **38.3 million raw taxi trip records** from NYC TLC, cleans them, enriches them with zone reference data, filters out invalid records, and produces analytics-ready tables for business consumption.

**Input**: Raw parquet data loaded into `NYC_TAXI_DB.RAW.yellow_tripdata`  
**Output**: 4 mart tables in `NYC_TAXI_DB.MARTS` schema — ready for dashboards and analysis

---

## Pipeline Flow

```
RAW DATA (38.3M rows)
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  STAGING (views in NYC_TAXI_DB.STAGING)                 │
│                                                         │
│  stg_yellow_trips                                       │
│  ├── Rename: PULocationID → pickup_location_id          │
│  ├── Rename: DOLocationID → dropoff_location_id         │
│  ├── Rename: tpep_pickup_datetime → pickup_datetime     │
│  ├── Cast: passenger_count FLOAT → INTEGER              │
│  ├── Cast: payment_type FLOAT → INTEGER                 │
│  ├── Compute: trip_duration_minutes (DATEDIFF)          │
│  ├── Generate: trip_id (surrogate key via MD5 hash)     │
│  └── Deduplicate: ROW_NUMBER to remove 1,804 true dupes │
│                                                         │
│  stg_taxi_zones                                         │
│  ├── Source: seeds/taxi_zone_lookup.csv (265 zones)     │
│  ├── Rename: LocationID → location_id                   │
│  └── Trim: whitespace on all string fields              │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  INTERMEDIATE (views in NYC_TAXI_DB.INTERMEDIATE)       │
│                                                         │
│  int_trips_enriched                                     │
│  ├── JOIN: stg_taxi_zones as pickup_zone                │
│  │   (pickup_location_id → location_id)                 │
│  │   Adds: pickup_borough, pickup_zone_name,            │
│  │         pickup_service_zone                          │
│  ├── JOIN: stg_taxi_zones as dropoff_zone               │
│  │   (dropoff_location_id → location_id)                │
│  │   Adds: dropoff_borough, dropoff_zone_name,          │
│  │         dropoff_service_zone                         │
│  └── FILTER (removes ~2.84M invalid records):           │
│      ├── trip_distance > 0                              │
│      ├── fare_amount > 0                                │
│      ├── passenger_count > 0 AND NOT NULL               │
│      ├── trip_duration_minutes >= 1                     │
│      └── trip_duration_minutes <= 180                   │
│                                                         │
│  38.3M → 35.47M rows (7.4% filtered out)               │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  MARTS (tables in NYC_TAXI_DB.MARTS)                    │
│                                                         │
│  fct_trips (35,468,133 rows)                            │
│  └── All valid, enriched trips. Core fact table.        │
│      Grain: one row per trip.                           │
│                                                         │
│  dim_zones (265 rows)                                   │
│  └── Zone reference data: location_id, borough,         │
│      zone_name, service_zone                            │
│                                                         │
│  agg_daily_revenue (374 rows)                           │
│  └── Grain: one row per calendar day                    │
│      Metrics: total_trips, total_fare, avg_fare,        │
│               total_tips, tip_rate_pct                  │
│                                                         │
│  agg_zone_performance (3,076 rows)                      │
│  └── Grain: one row per pickup zone per month           │
│      Metrics: total_trips, avg_trip_distance, avg_fare, │
│               total_revenue, revenue_rank,              │
│               is_high_volume                            │
│      Ranking: RANK() PARTITION BY month                 │
│      Flag: is_high_volume = TRUE if > 10K trips/month   │
└─────────────────────────────────────────────────────────┘
```

---

## Model Dependency Graph

```
sources:
  nyc_taxi.yellow_tripdata ──┐
                             ▼
seeds:                   stg_yellow_trips ──┐
  taxi_zone_lookup ──┐                      │
                     ▼                      ▼
               stg_taxi_zones ──┬── int_trips_enriched
                     │          │
                     │          ├──→ fct_trips ──┬──→ agg_daily_revenue
                     │          │                └──→ agg_zone_performance
                     └──────────┴──→ dim_zones
```

---

## Schema Changes: Raw → Staging

| Raw Column (as loaded) | Staging Column | Transformation |
|---|---|---|
| `VendorID` | `vendor_id` | Rename to snake_case |
| `tpep_pickup_datetime` | `pickup_datetime` | Rename (shorten) |
| `tpep_dropoff_datetime` | `dropoff_datetime` | Rename (shorten) |
| `passenger_count` | `passenger_count` | Cast FLOAT → INTEGER |
| `trip_distance` | `trip_distance` | No change |
| `RatecodeID` | `ratecode_id` | Cast FLOAT → INTEGER, rename |
| `store_and_fwd_flag` | `store_and_fwd_flag` | No change |
| `PULocationID` | `pickup_location_id` | Rename (expand abbreviation) |
| `DOLocationID` | `dropoff_location_id` | Rename (expand abbreviation) |
| `payment_type` | `payment_type` | Cast FLOAT → INTEGER |
| `fare_amount` | `fare_amount` | No change |
| `extra` | `extra` | No change |
| `mta_tax` | `mta_tax` | No change |
| `tip_amount` | `tip_amount` | No change |
| `tolls_amount` | `tolls_amount` | No change |
| `improvement_surcharge` | `improvement_surcharge` | No change |
| `total_amount` | `total_amount` | No change |
| `congestion_surcharge` | `congestion_surcharge` | No change |
| `airport_fee` | `airport_fee` | No change |
| *(not in raw)* | `trip_id` | **New**: surrogate key (MD5 hash of 10 fields) |
| *(not in raw)* | `trip_duration_minutes` | **New**: DATEDIFF(pickup, dropoff) / 60 |

---

## Columns Added by Intermediate Layer

| Column | Source | Join Condition |
|---|---|---|
| `pickup_borough` | stg_taxi_zones.borough | pickup_location_id = location_id |
| `pickup_zone_name` | stg_taxi_zones.zone_name | pickup_location_id = location_id |
| `pickup_service_zone` | stg_taxi_zones.service_zone | pickup_location_id = location_id |
| `dropoff_borough` | stg_taxi_zones.borough | dropoff_location_id = location_id |
| `dropoff_zone_name` | stg_taxi_zones.zone_name | dropoff_location_id = location_id |
| `dropoff_service_zone` | stg_taxi_zones.service_zone | dropoff_location_id = location_id |

Join type: **LEFT JOIN** — preserves trips even if zone lookup fails (NULL zone fields rather than dropped rows).

---

## Records Filtered Out (Intermediate Layer)

| Filter Rule | Why |
|---|---|
| `trip_distance <= 0` | Meter error or test record — no real trip occurred |
| `fare_amount <= 0` | No valid fare — test record, dispute, or system error |
| `passenger_count = 0 or NULL` | Driver did not enter count — data quality issue |
| `trip_duration_minutes < 1` | Less than 60 seconds — not a real trip |
| `trip_duration_minutes > 180` | Over 3 hours — likely meter left running |

**Impact**: ~2.84 million rows removed (7.4% of raw data).

---

## Materialization Strategy

| Layer | Materialization | Schema | Why |
|---|---|---|---|
| Staging | View | `STAGING` | Zero storage cost, always reflects source, no direct consumers |
| Intermediate | View | `INTERMEDIATE` | Transient transform, no query consumers |
| Marts | Table | `MARTS` | Pre-computed for analyst query performance |
| Seeds | Table | `RAW` | Small reference data loaded by `dbt seed` |

---

## Surrogate Key Design

The raw data has no single unique identifier. `trip_id` is generated by hashing 10 fields:

```
trip_id = MD5(
    tpep_pickup_datetime ||
    tpep_dropoff_datetime ||
    PULocationID ||
    DOLocationID ||
    fare_amount ||
    trip_distance ||
    vendorid ||
    payment_type ||
    tip_amount ||
    tolls_amount
)
```

Even with 10 fields, 1,804 source rows are true duplicates (every column identical). These are removed by `ROW_NUMBER() PARTITION BY trip_id ORDER BY pickup_datetime` — keeping only the first occurrence.

---

## Data Quality Tests (30 total — all passing)

### Built-in Tests

| Test | Model | Column | Type |
|---|---|---|---|
| not_null | stg_yellow_trips | trip_id, pickup/dropoff_datetime, pickup/dropoff_location_id | Schema |
| unique | stg_yellow_trips | trip_id | Schema |
| accepted_values | stg_yellow_trips | payment_type (0-5) | Schema (warn) |
| not_null | stg_taxi_zones | location_id, zone_name | Schema |
| unique | stg_taxi_zones | location_id | Schema |
| not_null | int_trips_enriched | trip_id | Schema |
| unique | int_trips_enriched | trip_id | Schema |
| not_null | fct_trips | trip_id, pickup/dropoff_datetime, pickup/dropoff_location_id | Schema |
| unique | fct_trips | trip_id | Schema |
| relationships | fct_trips | pickup_location_id → dim_zones.location_id | Referential integrity |
| relationships | fct_trips | dropoff_location_id → dim_zones.location_id | Referential integrity |
| not_null + unique | dim_zones | location_id | Schema |
| not_null | dim_zones | borough, zone_name | Schema |
| not_null + unique | agg_daily_revenue | trip_date | Schema |
| not_null | agg_zone_performance | pickup_location_id, trip_month | Schema |

### Custom Singular Test

**`assert_total_gte_fare`** — Business rule: `total_amount >= fare_amount` for every trip. Total includes fare + tips + surcharges + tolls, so it must always be at least as much as the base fare. Returns any violating rows. **Result: 0 violations (PASS).**

### Custom Generic Test

**`test_value_in_range`** — Parametrised test accepting `min_value` and `max_value`. Applied to `fct_trips.trip_duration_minutes` with range [1, 180]. Reusable on any numeric column. **Result: 0 violations (PASS).**

---

## Brainstormer: Revenue Rank — Monthly vs. Overall

**Question**: Should `agg_zone_performance` rank zones by revenue across the entire dataset, or within each month?

**Decision**: Rank within each month — `RANK() OVER (PARTITION BY trip_month ORDER BY total_revenue DESC)`.

**Reasoning**: An overall rank produces a single static number that hides seasonal patterns. Times Square might rank #1 overall, but that masks the fact that it drops significantly in February and surges in December. Monthly ranking reveals:

- **Seasonal trends** — which zones peak in summer vs. winter
- **Operational planning** — where to allocate more taxis each month
- **Performance tracking** — is a zone improving or declining month-over-month
- **Anomaly detection** — a zone dropping 20 rank positions in one month signals something worth investigating

A monthly grain with per-month ranking gives business stakeholders an actionable metric for resource allocation decisions, not just a static leaderboard.

---

## Running the Pipeline

```bash
cd dbt

# Install packages (dbt_utils)
dbt deps

# Load zone lookup seed
dbt seed

# Build all models (staging → intermediate → marts)
dbt run

# Run all 30 tests
dbt test

# Check source freshness
dbt source freshness

# Generate and view documentation
dbt docs generate
dbt docs serve
```

### Selective Runs

```bash
# Run only staging layer
dbt run --select staging

# Run a specific model and its downstream dependents
dbt run --select fct_trips+

# Run tests for marts only
dbt test --select marts
```

---

## File Structure

```
dbt/
├── models/
│   ├── staging/
│   │   ├── stg_yellow_trips.sql        # Raw → clean, rename, cast, dedup
│   │   ├── stg_taxi_zones.sql          # Seed wrapper, trim strings
│   │   ├── _stg__sources.yml           # Source definition + freshness
│   │   └── _stg__models.yml            # Staging model docs + tests
│   ├── intermediate/
│   │   ├── int_trips_enriched.sql      # Zone joins + quality filters
│   │   └── _int__models.yml            # Intermediate model docs + tests
│   └── marts/
│       ├── fct_trips.sql               # Fact table — all valid trips
│       ├── dim_zones.sql               # Dimension — zone reference
│       ├── agg_daily_revenue.sql       # Daily aggregation
│       ├── agg_zone_performance.sql    # Zone metrics + rank + flag
│       └── _marts__models.yml          # Mart model docs + tests
├── tests/
│   ├── assert_total_gte_fare.sql       # Singular: total >= fare
│   └── generic/
│       └── test_value_in_range.sql     # Generic: configurable min/max
├── seeds/
│   └── taxi_zone_lookup.csv            # 265 NYC taxi zones
├── macros/
│   └── generate_schema_name.sql        # Custom schema routing override
├── dbt_project.yml                     # Project config + materializations
├── packages.yml                        # dbt_utils dependency
├── profiles.yml.example                # Snowflake connection template
└── README.md                           # ← You are here
```