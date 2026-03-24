-- =============================================================================
-- Q3: Consecutive Trip Gap Analysis
-- =============================================================================
-- Purpose:  For each day in 2023, find the maximum gap (in minutes) between
--           the end of one trip and the start of the next trip originating
--           from the same pickup zone. Identifies periods of low demand.
-- Output:   trip_date, zone with largest gap, max gap in minutes
--
-- Performance Strategy for 38M Rows:
-- ---------------------------------------------------------------
-- This query is inherently expensive because it requires:
--   1. Sorting 38M rows by (zone, day, pickup_time) for the window function
--   2. Computing LAG() across every row within each partition
--
-- Snowflake-specific optimizations that would improve performance:
--
-- CLUSTERING KEY: ALTER TABLE yellow_tripdata CLUSTER BY (pulocationid,
--   tpep_pickup_datetime::DATE). This physically co-locates rows by zone
--   and date, which directly matches our PARTITION BY / ORDER BY pattern.
--   The window function can then process each partition from contiguous
--   micro-partitions instead of scanning the entire table.
--
-- SEARCH OPTIMIZATION: ALTER TABLE yellow_tripdata ADD SEARCH OPTIMIZATION
--   ON EQUALITY(pulocationid), SUBSTRING(tpep_pickup_datetime).
--   Helps when filtering to specific zones or date ranges.
--
-- MATERIALIZED VIEW: For repeated analysis, create a materialized view
--   pre-computing the LAG values. Snowflake maintains it incrementally:
--   CREATE MATERIALIZED VIEW mv_trip_gaps AS <this query's inner CTEs>
--   This shifts compute from query time to maintenance time.
--
-- RESULT CACHING: After first execution, identical queries return cached
--   results in <1s for 24 hours (if underlying data hasn't changed).
--
-- WAREHOUSE SIZING: For a one-time run, temporarily scaling to MEDIUM or
--   LARGE warehouse would parallelize the sort/window operation across more
--   nodes. ALTER WAREHOUSE NYC_TAXI_WH SET WAREHOUSE_SIZE = 'MEDIUM';
--   Then scale back down after the query completes.
--
-- Estimated runtime on X-Small: 60-120 seconds (sort-heavy on 38M rows)
-- With clustering key applied: 15-30 seconds
-- =============================================================================

with trip_sequence as (

    select
        pulocationid as pickup_location_id,
        cast(tpep_pickup_datetime as date) as trip_date,
        tpep_pickup_datetime as pickup_time,
        tpep_dropoff_datetime as dropoff_time,

        -- Previous trip's dropoff time within the same zone on the same day
        lag(tpep_dropoff_datetime) over (
            partition by pulocationid, cast(tpep_pickup_datetime as date)
            order by tpep_pickup_datetime
        ) as prev_dropoff_time

    from nyc_taxi_db.raw.yellow_tripdata

    where
        tpep_pickup_datetime >= '2023-01-01'
        and tpep_pickup_datetime < '2024-01-01'
        and trip_distance > 0
        and fare_amount > 0

),

trip_gaps as (

    select
        pickup_location_id,
        trip_date,
        -- Gap = time between previous trip ending and current trip starting
        round(
            datediff('second', prev_dropoff_time, pickup_time) / 60.0,
            2
        ) as gap_minutes

    from trip_sequence

    -- Only rows where a previous trip exists (skip first trip of each zone-day)
    where prev_dropoff_time is not null
      -- Filter out negative gaps (overlapping trips — dropoff after next pickup)
      and pickup_time > prev_dropoff_time

),

max_gap_per_zone_day as (

    select
        trip_date,
        pickup_location_id,
        max(gap_minutes) as max_gap_minutes

    from trip_gaps
    group by trip_date, pickup_location_id

),

-- For each day, find the zone with the single largest gap
daily_max_gap as (

    select
        mgzd.*,
        tz.zone as zone_name,
        tz.borough,
        row_number() over (
            partition by trip_date
            order by max_gap_minutes desc
        ) as rn

    from max_gap_per_zone_day mgzd

    left join nyc_taxi_db.raw.taxi_zone_lookup tz
        on mgzd.pickup_location_id = tz.locationid

)

select
    trip_date,
    zone_name,
    borough,
    pickup_location_id,
    max_gap_minutes

from daily_max_gap
where rn = 1

order by trip_date;