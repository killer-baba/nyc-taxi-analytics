# Execution Guide — Step-by-Step Setup

This guide walks you through setting up and running the entire project from scratch.

---

## Prerequisites

- **Python 3.11+** — [Download](https://www.python.org/downloads/)
- **Git** — [Download](https://git-scm.com/downloads)
- **VS Code** (recommended) — [Download](https://code.visualstudio.com/)
- **Snowflake account** — [Free 30-day trial](https://signup.snowflake.com/)

---

## Phase 1: Local Environment

### 1.1 Clone the Repository

```bash
git clone https://github.com/killer-baba/nyc-taxi-analytics.git
cd nyc-taxi-analytics
```

### 1.2 Create and Activate Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

You should see `(venv)` in your terminal prompt.

### 1.3 Install Dependencies

```bash
pip install -r requirements.txt
```

### 1.4 Verify DBT Installation

```bash
dbt --version
```

Expected: `dbt-core` and `dbt-snowflake` versions displayed.

---

## Phase 2: Snowflake Setup

### 2.1 Create Database and Schemas

Open a Snowflake worksheet and run `setup_scripts/01_snowflake_setup.sql`. This creates:
- Database: `NYC_TAXI_DB`
- Warehouse: `NYC_TAXI_WH` (X-Small, auto-suspend after 5 min)
- Schemas: `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`

### 2.2 Download Taxi Data

```bash
python setup_scripts/02_load_data.py
```

Downloads 12 monthly Parquet files + zone lookup CSV (~3-4 GB) into a local `data/` folder.

### 2.3 Set Snowflake Credentials

```bash
# Windows
set SNOWFLAKE_ACCOUNT=your-account-identifier
set SNOWFLAKE_USER=your-username
set SNOWFLAKE_PASSWORD=your-password

# macOS/Linux
export SNOWFLAKE_ACCOUNT=your-account-identifier
export SNOWFLAKE_USER=your-username
export SNOWFLAKE_PASSWORD=your-password
```

Your account identifier is in your Snowflake URL: `https://<account-identifier>.snowflakecomputing.com`

### 2.4 Load Data into Snowflake

```bash
python setup_scripts/03_load_to_snowflake.py
```

Uploads Parquet files to a Snowflake internal stage and loads into `RAW.yellow_tripdata`. Expected output: ~38.3 million rows loaded.

---

## Phase 3: Configure DBT

### 3.1 Create DBT Profile

```bash
# Windows
copy dbt\profiles.yml.example %USERPROFILE%\.dbt\profiles.yml

# macOS/Linux
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
```

Edit `~/.dbt/profiles.yml` — replace the `env_var()` placeholders with your Snowflake credentials, or set the environment variables referenced in the file.

### 3.2 Verify Connection

```bash
cd dbt
dbt debug
```

All checks should show `[OK]`. If connection fails, verify your account identifier, credentials, and that the warehouse `NYC_TAXI_WH` exists.

### 3.3 Install DBT Packages

```bash
dbt deps
```

Downloads `dbt_utils` and any other package dependencies.

### 3.4 Load Seed Data

```bash
dbt seed
```

Loads `seeds/taxi_zone_lookup.csv` into Snowflake as `RAW.taxi_zone_lookup` (~265 rows).

---

## Phase 4: Run DBT Pipeline

### 4.1 Run All Models

```bash
dbt run
```

Builds all models in dependency order: staging → intermediate → marts. Expected output:
- `staging.stg_yellow_trips` — view created
- `staging.stg_taxi_zones` — view created
- `intermediate.int_trips_enriched` — view created
- `marts.fct_trips` — 35,468,133 rows
- `marts.dim_zones` — 265 rows
- `marts.agg_daily_revenue` — 374 rows
- `marts.agg_zone_performance` — 3,076 rows

### 4.2 Run Tests

```bash
dbt test
```

Executes all 30 data quality tests. All should pass (PASS=30, ERROR=0).

### 4.3 Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

Opens an interactive documentation site with the full model dependency graph at `http://localhost:8080`.

---

## Phase 5: Run SQL Queries

The standalone analytical queries in `queries/` can be run directly in a Snowflake worksheet.

### Setup

```sql
USE DATABASE NYC_TAXI_DB;
USE WAREHOUSE NYC_TAXI_WH;
```

### Run Each Query

1. **Q1 — Top 10 Zones by Revenue per Month**
   - File: `queries/q1_top_zones_by_revenue.sql`
   - Expected: 120 rows (10 zones × 12 months)
   - Runtime: ~2.4 seconds

2. **Q2 — Hour-of-Day Demand Pattern**
   - File: `queries/q2_hour_of_day_pattern.sql`
   - Expected: 24 rows (one per hour)
   - Runtime: ~722ms

3. **Q3 — Consecutive Trip Gap Analysis**
   - File: `queries/q3_consecutive_gap_analysis.sql`
   - Expected: 365 rows (one per day of 2023)
   - Runtime: ~3 seconds

---

## Phase 6: Airflow (Local Testing)

### 6.1 Install Airflow

Airflow is included in `requirements.txt`. Initialize the database:

```bash
set AIRFLOW_HOME=%cd%\airflow_home  # for CMD Windows

airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
```

### 6.2 Configure DAG Path

```bash
# Copy the DAG to Airflow's dags folder
mkdir %AIRFLOW_HOME%\dags
copy dags\nyc_taxi_daily_pipeline.py %AIRFLOW_HOME%\dags\
```

Set the dbt project path as an environment variable:

```bash
# Windows
set DBT_PROJECT_DIR=C:\path\to\nyc-taxi-analytics\dbt

# macOS/Linux
export DBT_PROJECT_DIR=/path/to/nyc-taxi-analytics/dbt
```

### 6.3 Start Airflow

```bash
# Terminal 1: Start the scheduler
airflow scheduler

# Terminal 2: Start the web server
airflow webserver --port 8080
```

Open `http://localhost:8080` — the `nyc_taxi_daily_pipeline` DAG should appear. You can trigger a manual run from the UI.

### 6.4 Verify Task Execution

The DAG runs these tasks in order:
1. `check_source_freshness` — dbt source freshness
2. `run_dbt_staging` — dbt run --select staging
3. `run_dbt_intermediate` — dbt run --select intermediate
4. `run_dbt_marts` — dbt run --select marts
5. `run_dbt_tests` — dbt test (fails DAG if any test fails)
6. `notify_success` — logs summary

---

## Phase 7: Spark (Bonus — Local Testing)

The PySpark script processes historical data. It can be tested locally with the 2023 data subset.

### 7.1 Run Locally

```bash
spark-submit spark/process_historical.py --input data/ --output output/daily_revenue
```

This reads the 12 parquet files from `data/`, applies cleaning and filtering, computes daily revenue, and writes partitioned output to `output/daily_revenue/`.

### 7.2 Verify Output

```bash
# Check output structure
ls output/daily_revenue/
# Should see: year=2023/

ls output/daily_revenue/year=2023/
# Should see: month=1/, month=2/, ..., month=12/
```

### 7.3 Full-Scale Execution (AWS)

For the full 2009-2023 dataset on EMR:

```bash
spark-submit spark/process_historical.py \
    --input "s3://nyc-tlc/trip data/yellow_tripdata_*.parquet" \
    --output s3://my-bucket/nyc-taxi/daily_revenue
```

See [`spark/README.md`](spark/README.md) for EMR cluster configuration and cost estimates.

---

## Troubleshooting

### `dbt debug` fails with connection error
- Verify your Snowflake account identifier format (check your URL)
- Ensure warehouse `NYC_TAXI_WH` is created and not suspended
- Check that environment variables are set in the current terminal session

### Timestamps show "Invalid date" after loading
- Ensure the COPY INTO command uses `FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)`
- Without `USE_LOGICAL_TYPE`, Snowflake misinterprets microsecond timestamps from Parquet files
- The `02_load_to_snowflake.py` script handles this automatically

### `dbt test` shows unique test failures on trip_id
- This was resolved by deduplication in `stg_yellow_trips.sql`
- Source data contains ~1,804 true duplicate rows (every column identical)
- `ROW_NUMBER()` keeps only the first occurrence per trip_id

### PUT command fails with access denied
- The NYC TLC S3 bucket restricts direct Snowflake stage access
- Use the Python loader script (`setup_scripts/02_load_to_snowflake.py`) which downloads files locally first, then uses PUT via the Snowflake Python connector

### Airflow cannot find dbt
- Ensure the `DBT_PROJECT_DIR` environment variable points to the `dbt/` folder
- Ensure dbt is installed in the same Python environment as Airflow