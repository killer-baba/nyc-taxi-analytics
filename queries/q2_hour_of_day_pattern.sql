-- =============================================================================
-- Q2: Hour-of-Day Demand Pattern
-- =============================================================================
-- Purpose:  Analyze trip volume and pricing patterns across the 24-hour cycle
-- Output:   hour (0-23), total trips, avg fare, avg tip %, 3-hour rolling avg
-- Method:   Hourly aggregation + window function for rolling average
--
-- The 3-hour rolling average smooths out noise to reveal broader demand trends
-- (e.g., morning rush, afternoon lull, evening surge). The window frame
-- ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING creates a centered 3-hour window:
-- for hour 8, it averages hours 7, 8, and 9.
-- =============================================================================

with hourly_metrics as (

    select
        extract(hour from tpep_pickup_datetime) as pickup_hour,

        count(*) as total_trips,

        round(avg(fare_amount), 2) as avg_fare,

        -- Avg tip percentage: tip as % of fare (excluding zero-fare trips)
        round(
            avg(
                case
                    when fare_amount > 0 then (tip_amount / fare_amount) * 100
                    else null
                end
            ),
            2
        ) as avg_tip_percentage

    from nyc_taxi_db.raw.yellow_tripdata

    where
        tpep_pickup_datetime >= '2023-01-01'
        and tpep_pickup_datetime < '2024-01-01'
        and fare_amount > 0
        and passenger_count > 0

    group by extract(hour from tpep_pickup_datetime)

),

with_rolling_avg as (

    select
        pickup_hour,
        total_trips,
        avg_fare,
        avg_tip_percentage,

        -- 3-hour centered rolling average of trip count
        -- For hour 0: averages hours 23(?), 0, 1 — but since we only have
        -- 0-23 in the result set, the window naturally handles edge hours
        -- by averaging fewer rows (hour 0 = avg of 0,1; hour 23 = avg of 22,23)
        round(
            avg(total_trips) over (
                order by pickup_hour
                rows between 1 preceding and 1 following
            ),
            0
        ) as rolling_3hr_avg_trips

    from hourly_metrics

)

select *
from with_rolling_avg
order by pickup_hour;