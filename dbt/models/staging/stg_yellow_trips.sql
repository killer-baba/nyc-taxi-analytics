-- =============================================================================
-- stg_yellow_trips: Staging model for NYC Yellow Taxi trip records
-- =============================================================================
-- Purpose:  Clean interface over raw yellow_tripdata
-- Grain:    One row per trip record (deduplicated)
-- Source:   {{ source('nyc_taxi', 'yellow_tripdata') }}
-- Layer:    Staging (view)
--
-- Transformations applied:
--   - Column renaming: PascalCase/abbreviations → consistent snake_case
--   - Type casting: FLOAT → INTEGER for count/category columns
--   - Computed column: trip_duration_minutes from pickup/dropoff timestamps
--   - Surrogate key: deterministic hash for row-level identification
--   - Deduplication: ROW_NUMBER to handle true duplicate rows in source
-- =============================================================================

with source as (

    select * from {{ source('nyc_taxi', 'yellow_tripdata') }}

),

renamed as (

    select
        -- Surrogate key: hash of fields that together identify a unique trip
        {{ dbt_utils.generate_surrogate_key([
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'fare_amount',
            'trip_distance',
            'vendorid',
            'payment_type',
            'tip_amount',
            'tolls_amount'
        ]) }} as trip_id,

        -- Vendor
        vendorid as vendor_id,

        -- Timestamps (already TIMESTAMP_NTZ from parquet load)
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,

        -- Trip details — cast from FLOAT to INTEGER where appropriate
        cast(passenger_count as integer) as passenger_count,
        trip_distance,
        cast(ratecodeid as integer) as ratecode_id,
        store_and_fwd_flag,

        -- Location IDs
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,

        -- Payment
        cast(payment_type as integer) as payment_type,

        -- Financials
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,

        -- Computed: trip duration in minutes
        round(datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0, 2)
            as trip_duration_minutes

    from source

),

-- Deduplicate: source data contains true duplicate rows (every column identical).
-- ROW_NUMBER assigns 1 to the first occurrence per trip_id; we keep only that row.
deduplicated as (

    select
        *,
        row_number() over (
            partition by trip_id
            order by pickup_datetime
        ) as _row_num

    from renamed

)

select
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
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
    trip_duration_minutes

from deduplicated
where _row_num = 1