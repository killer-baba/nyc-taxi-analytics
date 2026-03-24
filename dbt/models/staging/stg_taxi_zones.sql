-- =============================================================================
-- stg_taxi_zones: Staging model for NYC TLC taxi zone reference data
-- =============================================================================
-- Purpose:  Clean interface over taxi zone lookup seed
-- Grain:    One row per taxi zone (265 zones)
-- Source:   seeds/taxi_zone_lookup.csv
-- Layer:    Staging (view)
--
-- Transformations applied:
--   - Column renaming to snake_case
--   - Trim whitespace from string fields
-- =============================================================================

with source as (

    select * from {{ ref('taxi_zone_lookup') }}

),

renamed as (

    select
        locationid as location_id,
        trim(borough) as borough,
        trim(zone) as zone_name,
        trim(service_zone) as service_zone

    from source

)

select * from renamed