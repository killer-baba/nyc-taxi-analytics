-- =============================================================================
-- Q1: Top 10 Pickup Zones by Revenue — Each Month of 2023
-- =============================================================================
-- Purpose:  Identify highest-revenue pickup zones per month
-- Output:   month, zone rank (1-10), zone name, borough, total revenue, trip count
-- Method:   CTE aggregation + RANK() window function
--
-- Performance Strategy (Snowflake X-Small, targeting < 30s):
-- ---------------------------------------------------------------
-- 1. PARTITION PRUNING: The WHERE clause on pickup_datetime filters to 2023.
--    Snowflake stores metadata (min/max) per micro-partition. Since data was
--    loaded chronologically (monthly parquet files), partitions outside 2023
--    are skipped entirely without scanning. This reduces I/O significantly.
--
-- 2. CLUSTERING CONSIDERATION: For repeated runs, adding a clustering key
--    on (tpep_pickup_datetime) would further optimize pruning. On a one-time
--    analysis like this, the natural load order already provides good pruning.
--
-- 3. AGGREGATION BEFORE WINDOW: We aggregate to zone-month grain first (CTE),
--    reducing 35M rows to ~3K rows before applying RANK(). The window function
--    operates on the small aggregated result, not the full dataset.
--
-- 4. RESULT CACHING: Snowflake caches query results for 24 hours. Subsequent
--    runs of this exact query return instantly from cache.
-- =============================================================================

with monthly_zone_revenue as (

    select
        date_trunc('month', tpep_pickup_datetime) as revenue_month,
        pulocationid as pickup_location_id,
        count(*) as total_trips,
        round(sum(total_amount), 2) as total_revenue,
        round(avg(total_amount), 2) as avg_revenue_per_trip

    from nyc_taxi_db.raw.yellow_tripdata

    where
        -- Filter to 2023 — enables partition pruning
        tpep_pickup_datetime >= '2023-01-01'
        and tpep_pickup_datetime < '2024-01-01'
        -- Basic quality filters
        and total_amount > 0
        and trip_distance > 0

    group by
        date_trunc('month', tpep_pickup_datetime),
        pulocationid

),

ranked as (

    select
        mzr.*,
        tz.zone as zone_name,
        tz.borough,
        rank() over (
            partition by revenue_month
            order by total_revenue desc
        ) as revenue_rank

    from monthly_zone_revenue mzr

    left join nyc_taxi_db.raw.taxi_zone_lookup tz
        on mzr.pickup_location_id = tz.locationid

)

select
    revenue_month,
    revenue_rank,
    zone_name,
    borough,
    total_trips,
    total_revenue,
    avg_revenue_per_trip

from ranked
where revenue_rank <= 10

order by revenue_month, revenue_rank;