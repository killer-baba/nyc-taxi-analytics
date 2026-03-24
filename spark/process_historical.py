"""
NYC TLC Historical Data Processing — PySpark
==============================================
Processes all Yellow Taxi trip records from 2009 to 2023 (~1.5 billion rows).
Applies cleaning logic equivalent to the DBT staging model, computes daily
revenue aggregation, and writes partitioned Parquet output.

Usage (local):
    spark-submit spark/process_historical.py \
        --input data/ \
        --output output/daily_revenue

Usage (EMR/S3):
    spark-submit spark/process_historical.py \
        --input s3://nyc-tlc/trip\ data/ \
        --output s3://my-bucket/nyc-taxi/daily_revenue

Deployment Notes (AWS):
-----------------------
EMR:
    - Cluster config: m5.xlarge instances (4 vCPU, 16GB RAM each)
    - Recommended: 1 master + 4-8 core nodes for 1.5B rows
    - Use EMR Steps API to submit this script
    - Input directly from S3 (s3://nyc-tlc/trip data/)
    - Output to S3 with Hive-style partitioning (year=YYYY/month=MM/)
    - Enable dynamic allocation: spark.dynamicAllocation.enabled=true
    - Set spark.sql.shuffle.partitions=200 (default 200, increase for
      larger datasets or reduce for smaller clusters)

Glue:
    - Create a Glue ETL job pointing to this script
    - Use Glue's built-in Spark context (no cluster management)
    - Configure max DPUs based on data volume (~20 DPUs for 1.5B rows)
    - Glue handles S3 read/write natively
    - Downside: less control over Spark config vs EMR

Cost Estimate:
    - EMR: ~$2-4/hour for a 5-node m5.xlarge cluster
    - Processing 1.5B rows: estimated 30-60 minutes
    - Total: ~$2-4 for a one-time historical backfill
"""

import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType,
    TimestampType, StringType
)


def create_spark_session():
    """
    Initialize SparkSession with optimized configuration for large-scale
    Parquet processing.
    """
    return (
        SparkSession.builder
        .appName("nyc_taxi_historical_processing")
        # Adaptive Query Execution: lets Spark auto-optimize shuffle
        # partitions and join strategies at runtime based on actual data sizes
        .config("spark.sql.adaptive.enabled", "true")
        # Parquet pushdown: push filters into the Parquet reader so it
        # skips row groups that don't match — major I/O reduction
        .config("spark.sql.parquet.filterPushdown", "true")
        # Broadcast threshold: auto-broadcast tables under 100MB
        # Our zone lookup (265 rows) is well under this limit
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
        .getOrCreate()
    )


def define_schema():
    """
    Explicit schema definition for Yellow Taxi Parquet files.

    Why define schema explicitly instead of relying on Parquet inference?
    - Schema inference reads file footers, which is slow across 150+ files
    - Parquet schema can vary across years (columns added/removed over time)
    - Explicit schema ensures consistent column types across all years
    - Columns not present in older files will be filled with NULL
    """
    return StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", FloatType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("RatecodeID", FloatType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("improvement_surcharge", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("congestion_surcharge", FloatType(), True),
        StructField("airport_fee", FloatType(), True),
    ])


def read_trip_data(spark, input_path, schema):
    """
    Read all Yellow Taxi Parquet files from input path.

    mergeSchema=true handles schema evolution across years — older files
    (2009-2014) have fewer columns than newer ones. Spark will unify all
    schemas and fill missing columns with NULL.
    """
    return (
        spark.read
        .option("mergeSchema", "true")
        .schema(schema)
        .parquet(input_path)
    )


def read_zone_lookup(spark, zone_path):
    """
    Read the taxi zone lookup CSV.

    This DataFrame is small (265 rows, ~10KB) and will be broadcast
    to all worker nodes during the join to avoid shuffling the large
    trip DataFrame.
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zone_path)
    )


def clean_trips(df):
    """
    Apply cleaning logic equivalent to the DBT staging + intermediate models.

    Transformations:
        - Rename columns to snake_case
        - Cast types (passenger_count float → integer)
        - Compute trip_duration_minutes
        - Filter invalid records (same rules as int_trips_enriched)

    This mirrors the DBT pipeline logic so the Spark output is consistent
    with the warehouse-based transformation.
    """
    cleaned = (
        df
        # Rename columns to snake_case (matching DBT staging model)
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

        # Cast passenger_count from float to integer
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))

        # Compute trip duration in minutes
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
                2
            )
        )

        # Add year and month columns for partitioned output
        .withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
    )

    # Apply data quality filters (matching DBT intermediate layer)
    filtered = (
        cleaned
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("passenger_count").isNotNull())
        .filter(F.col("trip_duration_minutes") >= 1)
        .filter(F.col("trip_duration_minutes") <= 180)
        # Filter out records with null/invalid dates
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("year").between(2009, 2023))
    )

    return filtered


def compute_daily_revenue(df):
    """
    Compute daily revenue aggregation — equivalent to DBT's agg_daily_revenue.

    Metrics per day:
        - total_trips: count of trips
        - total_fare: sum of fare_amount
        - avg_fare: average fare per trip
        - total_tips: sum of tip_amount
        - tip_rate_pct: total tips / total fare * 100
    """
    return (
        df
        .groupBy(
            F.col("year"),
            F.col("month"),
            F.to_date("pickup_datetime").alias("trip_date")
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            # Tip rate: total tips / total fare * 100
            # coalesce handles edge case of zero fare days
            F.round(
                F.coalesce(
                    F.sum("tip_amount") / F.sum("fare_amount") * 100,
                    F.lit(0)
                ),
                2
            ).alias("tip_rate_pct"),
        )
        .orderBy("trip_date")
    )


def write_output(df, output_path):
    """
    Write aggregated output as partitioned Parquet files.

    Partitioning by year/month creates a directory structure:
        output/year=2023/month=1/part-00000.parquet
        output/year=2023/month=2/part-00000.parquet
        ...

    Why partition by year/month?
    - Downstream queries filtering by date range read only relevant partitions
    - Hive/Athena/Snowflake external tables can leverage partition pruning
    - Each partition file is small enough for efficient processing
    - Standard pattern for time-series data in data lakes

    Why repartition before writing?
    - Without repartition, Spark's internal partitioning may spread a single
      month's data across many small files (file fragmentation)
    - Repartitioning by (year, month) ensures each output partition contains
      all data for that year-month in a single file
    - coalesce(1) within each partition would give exactly 1 file per
      partition, but repartition is sufficient for this use case
    """
    (
        df
        .repartition("year", "month")
        .write
        .partitionBy("year", "month")
        .mode("overwrite")
        .parquet(output_path)
    )


def main():
    parser = argparse.ArgumentParser(
        description="Process historical NYC TLC Yellow Taxi data"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input Parquet files (local dir or S3 prefix)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path for output partitioned Parquet (local dir or S3 prefix)"
    )
    parser.add_argument(
        "--zones",
        default=None,
        help="Path to taxi_zone_lookup.csv (optional, for zone-enriched output)"
    )
    args = parser.parse_args()

    # ── Initialize Spark ──
    spark = create_spark_session()

    try:
        # ── Read raw trip data ──
        print(f"Reading trip data from: {args.input}")
        schema = define_schema()
        trips_raw = read_trip_data(spark, args.input, schema)

        raw_count = trips_raw.count()
        print(f"Raw records loaded: {raw_count:,}")

        # ── Clean and filter ──
        print("Applying cleaning and quality filters...")
        trips_clean = clean_trips(trips_raw)

        # Cache the cleaned DataFrame because we use it for both
        # the daily aggregation and the row count check.
        # Without cache, Spark would re-read and re-clean the parquet
        # files for each action (count + aggregation).
        trips_clean.cache()

        clean_count = trips_clean.count()
        filtered_pct = round((1 - clean_count / raw_count) * 100, 1)
        print(f"Clean records: {clean_count:,} ({filtered_pct}% filtered out)")

        # ── Compute daily revenue aggregation ──
        print("Computing daily revenue aggregation...")
        daily_revenue = compute_daily_revenue(trips_clean)

        # ── Write partitioned output ──
        print(f"Writing output to: {args.output}")
        write_output(daily_revenue, args.output)

        # ── Summary ──
        output_count = daily_revenue.count()
        print(f"\nProcessing complete.")
        print(f"  Input:    {raw_count:,} raw records")
        print(f"  Cleaned:  {clean_count:,} valid records")
        print(f"  Output:   {output_count:,} daily aggregation rows")
        print(f"  Written:  {args.output} (partitioned by year/month)")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()