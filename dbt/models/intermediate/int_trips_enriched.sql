-- =============================================================================
-- int_trips_enriched: Intermediate model — enriched and validated trips
-- =============================================================================
-- Purpose:  Join trips with zone reference data and filter invalid records
-- Grain:    One row per valid trip
-- Upstream: stg_yellow_trips, stg_taxi_zones
-- Layer:    Intermediate (view)
--
-- Enrichment:
--   - Pickup zone name and borough resolved from location ID
--   - Dropoff zone name and borough resolved from location ID
--
-- Filtering (invalid record removal):
--   - trip_distance <= 0       → meter error or test record
--   - fare_amount <= 0         → no valid fare recorded
--   - passenger_count = 0/NULL → driver did not enter count
--   - duration < 1 minute      → too short to be a real trip
--   - duration > 180 minutes   → likely meter left running
-- =============================================================================

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

zones as (

    select * from {{ ref('stg_taxi_zones') }}

),

enriched as (

    select
        trips.trip_id,
        trips.vendor_id,
        trips.pickup_datetime,
        trips.dropoff_datetime,
        trips.trip_duration_minutes,
        trips.passenger_count,
        trips.trip_distance,
        trips.ratecode_id,
        trips.store_and_fwd_flag,
        trips.pickup_location_id,
        trips.dropoff_location_id,
        trips.payment_type,
        trips.fare_amount,
        trips.extra,
        trips.mta_tax,
        trips.tip_amount,
        trips.tolls_amount,
        trips.improvement_surcharge,
        trips.total_amount,
        trips.congestion_surcharge,
        trips.airport_fee,

        -- Enriched: pickup zone details
        pickup_zone.borough as pickup_borough,
        pickup_zone.zone_name as pickup_zone_name,
        pickup_zone.service_zone as pickup_service_zone,

        -- Enriched: dropoff zone details
        dropoff_zone.borough as dropoff_borough,
        dropoff_zone.zone_name as dropoff_zone_name,
        dropoff_zone.service_zone as dropoff_service_zone

    from trips

    left join zones as pickup_zone
        on trips.pickup_location_id = pickup_zone.location_id

    left join zones as dropoff_zone
        on trips.dropoff_location_id = dropoff_zone.location_id

),

filtered as (

    select * from enriched
    where
        -- Remove trips with no meaningful distance
        trip_distance > 0

        -- Remove trips with no valid fare
        and fare_amount > 0

        -- Remove trips with no passenger count recorded
        and passenger_count is not null
        and passenger_count > 0

        -- Remove unreasonable durations
        and trip_duration_minutes >= 1      -- minimum 1 minute
        and trip_duration_minutes <= 180    -- maximum 3 hours

)

select * from filtered