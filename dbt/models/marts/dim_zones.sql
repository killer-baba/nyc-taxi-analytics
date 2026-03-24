-- =============================================================================
-- dim_zones: Dimension table — NYC TLC taxi zones
-- =============================================================================
-- Purpose:  Reference table for taxi zone attributes
-- Grain:    One row per zone (265 zones)
-- Upstream: stg_taxi_zones
-- Layer:    Marts (table)
-- =============================================================================

select
    location_id,
    borough,
    zone_name,
    service_zone

from {{ ref('stg_taxi_zones') }}