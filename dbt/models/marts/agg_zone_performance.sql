-- =============================================================================
-- agg_zone_performance: Zone-level performance metrics
-- =============================================================================
-- Purpose:  Per pickup zone performance with revenue ranking and volume flag
-- Grain:    One row per pickup zone per month
-- Upstream: fct_trips, dim_zones
-- Layer:    Marts (table)
--
-- Brainstormer: Revenue Rank — Monthly vs. Overall
-- ------------------------------------------------
-- RANK() across the full dataset gives a single static ranking. This hides
-- seasonal variation: a zone like "Times Square" might dominate in December
-- (holiday tourism) but drop in February. An overall rank flattens this signal.
--
-- Ranking WITHIN each month captures temporal performance trends. Operations
-- teams can track whether a zone is improving or declining month-over-month.
-- Marketing can identify seasonal opportunities. Revenue forecasting gets
-- granular inputs.
--
-- Decision: Rank within each month (PARTITION BY month). This produces a more
-- actionable metric for business stakeholders who need to make monthly
-- resource allocation decisions (e.g., where to stage more taxis).
-- =============================================================================

with monthly_zone_metrics as (

    select
        pickup_location_id,
        pickup_zone_name,
        pickup_borough,
        date_trunc('month', pickup_datetime) as trip_month,

        count(*) as total_trips,
        round(avg(trip_distance), 2) as avg_trip_distance,
        round(avg(fare_amount), 2) as avg_fare,
        round(sum(total_amount), 2) as total_revenue

    from {{ ref('fct_trips') }}

    group by
        pickup_location_id,
        pickup_zone_name,
        pickup_borough,
        date_trunc('month', pickup_datetime)

),

ranked as (

    select
        *,

        -- Revenue rank within each month (see brainstormer note above)
        rank() over (
            partition by trip_month
            order by total_revenue desc
        ) as revenue_rank,

        -- High-volume flag: zones exceeding 10,000 trips in a given month
        case
            when total_trips > 10000 then true
            else false
        end as is_high_volume

    from monthly_zone_metrics

)

select * from ranked