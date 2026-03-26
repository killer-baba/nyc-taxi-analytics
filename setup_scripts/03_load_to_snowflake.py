"""
Load NYC TLC Yellow Taxi 2023 Parquet files into Snowflake.

Prerequisites:
    1. Run 01_snowflake_setup.sql in Snowflake worksheet
    2. Run 02_load_data.py to download parquet files into data/ folder
    3. Set environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

Usage:
    python setup_scripts/02_load_to_snowflake.py
"""

import os
import glob
import snowflake.connector

# ── Connection via environment variables (never hardcode credentials) ──
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    database="NYC_TAXI_DB",
    schema="RAW",
    warehouse="NYC_TAXI_WH",
)

cur = conn.cursor()

try:
    # ── Step 1: Create raw table matching NYC TLC parquet schema ──
    print("Creating raw table...")
    cur.execute("""
        CREATE OR REPLACE TABLE yellow_tripdata (
            VendorID                INTEGER,
            tpep_pickup_datetime    TIMESTAMP_NTZ,
            tpep_dropoff_datetime   TIMESTAMP_NTZ,
            passenger_count         FLOAT,
            trip_distance           FLOAT,
            RatecodeID              FLOAT,
            store_and_fwd_flag      VARCHAR,
            PULocationID            INTEGER,
            DOLocationID            INTEGER,
            payment_type            INTEGER,
            fare_amount             FLOAT,
            extra                   FLOAT,
            mta_tax                 FLOAT,
            tip_amount              FLOAT,
            tolls_amount            FLOAT,
            improvement_surcharge   FLOAT,
            total_amount            FLOAT,
            congestion_surcharge    FLOAT,
            airport_fee             FLOAT
        )
    """)
    print("Table created.")

    # ── Step 2: Create internal stage ──
    print("Creating internal stage...")
    cur.execute("""
        CREATE OR REPLACE STAGE yellow_taxi_stage
          FILE_FORMAT = (TYPE = PARQUET)
    """)
    print("Stage created.")

    # ── Step 3: Upload each parquet file via PUT ──
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    files = sorted(glob.glob(os.path.join(data_dir, "yellow_tripdata_2023-*.parquet")))

    if not files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}. Run load_data.py first.")

    print(f"Found {len(files)} parquet files. Uploading...")

    for filepath in files:
        filename = os.path.basename(filepath)
        # Normalize path for Snowflake PUT command (forward slashes)
        normalized_path = os.path.abspath(filepath).replace("\\", "/")
        put_command = f"PUT 'file://{normalized_path}' @yellow_taxi_stage AUTO_COMPRESS=FALSE"
        print(f"  Uploading {filename}...")
        cur.execute(put_command)
        print(f"  Done: {filename}")

    # ── Step 4: COPY INTO table from stage ──
    print("Loading data from stage into table...")
    cur.execute("""
        COPY INTO yellow_tripdata
        FROM @yellow_taxi_stage
          FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
          MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """)
    print("Data loaded.")

    # ── Step 5: Verify ──
    cur.execute("SELECT COUNT(*) FROM yellow_tripdata")
    row_count = cur.fetchone()[0]
    print(f"\nTotal rows loaded: {row_count:,}")

    cur.execute("""
        SELECT 
            MIN(tpep_pickup_datetime) AS earliest,
            MAX(tpep_pickup_datetime) AS latest
        FROM yellow_tripdata
    """)
    earliest, latest = cur.fetchone()
    print(f"Date range: {earliest} to {latest}")
    print("\nLoad complete. Verify row count is ~38M and dates span 2023.")

finally:
    cur.close()
    conn.close()
