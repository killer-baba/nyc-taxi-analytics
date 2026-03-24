-- =============================================================================
-- agg_daily_revenue: Daily revenue aggregation
-- =============================================================================
-- Purpose:  Pre-computed daily metrics for revenue dashboards
-- Grain:    One row per calendar day
-- Upstream: fct_trips
-- Layer:    Marts (table)
--
-- Metrics:
--   - total_trips:  count of trips that day
--   - total_fare:   sum of fare_amount
--   - avg_fare:     average fare per trip
--   - total_tips:   sum of tip_amount
--   - tip_rate_pct: tips as a percentage of total fare (not per-trip avg)
-- =============================================================================

select
    cast(pickup_datetime as date) as trip_date,

    count(*) as total_trips,
    round(sum(fare_amount), 2) as total_fare,
    round(avg(fare_amount), 2) as avg_fare,
    round(sum(tip_amount), 2) as total_tips,

    -- Tip rate: total tips / total fare * 100
    -- Using NULLIF to prevent division by zero on days with zero fare (unlikely but defensive)
    round(
        sum(tip_amount) / nullif(sum(fare_amount), 0) * 100,
        2
    ) as tip_rate_pct

from {{ ref('fct_trips') }}

group by cast(pickup_datetime as date)

order by trip_date