-- =============================================================================
-- fct_trips: Fact table — all valid, enriched yellow taxi trips
-- =============================================================================
-- Purpose:  Core fact table for the NYC taxi analytics mart
-- Grain:    One row per valid trip
-- Upstream: int_trips_enriched
-- Layer:    Marts (table)
--
-- This is the primary table for analysts and downstream consumers.
-- All data quality filters have been applied upstream in the intermediate layer.
-- =============================================================================

select
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    pickup_borough,
    pickup_zone_name,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone_name,
    dropoff_service_zone

from {{ ref('int_trips_enriched') }}