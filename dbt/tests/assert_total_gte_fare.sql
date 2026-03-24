-- =============================================================================
-- Singular test: total_amount must be >= fare_amount
-- =============================================================================
-- Business rule: total_amount = fare + tips + surcharges + tolls
-- Therefore total should always be >= fare. A violation indicates
-- data corruption, incorrect calculation, or loading error.
--
-- This query returns rows that FAIL the assertion.
-- DBT treats any rows returned as test failures.
-- =============================================================================

select
    trip_id,
    fare_amount,
    total_amount,
    total_amount - fare_amount as difference
from {{ ref('fct_trips') }}
where total_amount < fare_amount