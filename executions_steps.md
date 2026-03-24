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
git clone <repo-url>
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
python setup_scripts/load_data.py
```

Downloads 12 monthly Parquet files (~3-4 GB) into a local `data/` folder.

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
python setup_scripts/02_load_to_snowflake.py
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

Builds all models in dependency order: staging → intermediate → marts.

### 4.2 Run Tests

```bash
dbt test
```

Executes all built-in and custom tests. All should pass.

### 4.3 Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

Opens an interactive documentation site with the full model dependency graph.

---

## Phase 5: Run SQL Queries

The standalone analytical queries in `queries/` can be run directly in a Snowflake worksheet:

1. Open your Snowflake worksheet
2. Set context: `USE DATABASE NYC_TAXI_DB; USE WAREHOUSE NYC_TAXI_WH;`
3. Copy and run each query file:
   - `q1_top_zones_by_revenue.sql`
   - `q2_hour_of_day_pattern.sql`
   - `q3_consecutive_gap_analysis.sql`

---

## Phase 6: Airflow (Local Testing)

*Instructions will be added when the Airflow DAG is built.*

---

## Phase 7: Spark (Bonus)

The PySpark script in `spark/process_historical.py` is designed for cluster execution but can be tested locally:

```bash
python spark/process_historical.py --input data/ --output output/
```

*Full instructions will be added when the Spark script is built.*

---

## Troubleshooting

### `dbt debug` fails with connection error
- Verify your Snowflake account identifier format (check your URL)
- Ensure warehouse `NYC_TAXI_WH` is created and not suspended
- Check that environment variables are set in the current terminal session

### Timestamps show invalid dates after loading
- Ensure the COPY INTO command uses `FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)`
- Without `USE_LOGICAL_TYPE`, Snowflake misinterprets microsecond timestamps from Parquet files

### PUT command fails with access denied
- Ensure you're using the Python loader script, not a direct S3 stage
- The NYC TLC S3 bucket restricts direct Snowflake stage access