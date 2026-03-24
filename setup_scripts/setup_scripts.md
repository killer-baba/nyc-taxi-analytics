# Setup Scripts

One-time scripts to initialize the project environment. Run these in order before working with DBT or Airflow.

## Execution Order

### 1. `01_snowflake_setup.sql`
**Run in**: Snowflake worksheet  
**What it does**: Creates the `NYC_TAXI_DB` database, `NYC_TAXI_WH` warehouse (X-Small), and schemas (`RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`).

### 2. `load_data.py`
**Run in**: Terminal (with venv activated)  
**What it does**: Downloads 12 monthly Yellow Taxi Parquet files for 2023 from the NYC TLC public website into a local `data/` folder. ~3-4 GB total.

```bash
python setup_scripts/load_data.py
```

### 3. `02_load_to_snowflake.py`
**Run in**: Terminal (with venv activated)  
**What it does**: Connects to Snowflake, creates the `yellow_tripdata` raw table, uploads Parquet files via PUT to an internal stage, and loads data via COPY INTO. Verifies row count (~38M expected).

**Requires environment variables:**
```bash
set SNOWFLAKE_ACCOUNT=your-account-identifier
set SNOWFLAKE_USER=your-username
set SNOWFLAKE_PASSWORD=your-password
```

```bash
python setup_scripts/02_load_to_snowflake.py
```

## Notes

- The `data/` folder is in `.gitignore` — raw Parquet files are never committed.
- The load script uses `USE_LOGICAL_TYPE = TRUE` in the Parquet file format to correctly interpret timestamp columns. Without this, Snowflake misinterprets microsecond-precision timestamps, resulting in dates millions of years in the future.
- The load script is idempotent — running it again will recreate the table and reload from scratch (CREATE OR REPLACE).