# SQL Performance Queries

## Overview

Three standalone analytical SQL queries written in Snowflake dialect. These query the raw `yellow_tripdata` table directly (not the dbt mart tables) to demonstrate SQL proficiency and performance awareness independent of the transformation pipeline.

---

## Query Summary

| Query | Purpose | Rows Returned | Runtime (X-Small) | Key Technique |
|---|---|---|---|---|
| Q1 | Top 10 zones by revenue per month | 120 | ~2.4s | RANK() window function |
| Q2 | Hour-of-day demand pattern | 24 | ~3-5s | Rolling AVG window frame |
| Q3 | Consecutive trip gap analysis | ~365 | ~60-120s | LAG() window function |

---

## Q1 — Top 10 Pickup Zones by Revenue (Each Month)

**File**: `q1_top_zones_by_revenue.sql`

**What it answers**: Which pickup zones generate the most revenue, and how does this change month to month?

**Approach**:
1. Aggregate total revenue per zone per month (~3K rows from 35M)
2. RANK() within each month by total_revenue DESC
3. Filter to top 10 per month
4. Join zone names for readability

**Output columns**: revenue_month, revenue_rank, zone_name, borough, total_trips, total_revenue, avg_revenue_per_trip

**Performance strategy**:
- Range predicate on `tpep_pickup_datetime` enables partition pruning (Snowflake skips non-2023 micro-partitions)
- Aggregation reduces 35M rows to ~3K before RANK() is applied
- Avoid wrapping filter column in functions (no `YEAR(col)` — kills pruning)

**Verified result**: 120 rows, 2.4 seconds on X-Small. JFK Airport consistently ranks #1 due to high flat-rate fares ($77 avg vs $23 for Midtown).

---

## Q2 — Hour-of-Day Demand Pattern

**File**: `q2_hour_of_day_pattern.sql`

**What it answers**: What does NYC taxi demand look like across a 24-hour cycle? When are fares highest? When do people tip the most?

**Approach**:
1. Aggregate metrics per hour of day (24 groups from 35M rows)
2. Compute avg tip percentage using CASE to handle zero-fare edge cases
3. Apply 3-hour centered rolling average using window frame

**Output columns**: pickup_hour, total_trips, avg_fare, avg_tip_percentage, rolling_3hr_avg_trips

**Rolling average detail**: `AVG(total_trips) OVER (ORDER BY pickup_hour ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)` creates a centered 3-hour window. For hour 10, it averages hours 9, 10, 11. Edge hours (0 and 23) use a 2-hour window since there's no wrap-around.

**Expected patterns**:
- Morning rush (7-9 AM): high volume, moderate fares
- Midday: steady volume
- Evening (6-8 PM): highest volume, higher fares
- Late night (2-5 AM): lowest volume, potentially higher tip % (bar/restaurant closing)

---

## Q3 — Consecutive Trip Gap Analysis

**File**: `q3_consecutive_gap_analysis.sql`

**What it answers**: For each day in 2023, what was the longest period where no taxi was picked up from a zone? Identifies demand dead zones.

**Approach**:
1. LAG() to get previous trip's dropoff time within same zone and day
2. Calculate gap = current pickup - previous dropoff (in minutes)
3. Filter negative gaps (overlapping trips)
4. MAX gap per zone per day, then find the zone with the largest gap each day

**Output columns**: trip_date, zone_name, borough, pickup_location_id, max_gap_minutes

**Why this is the hardest query**: LAG() requires sorting 35M rows by (zone, date, pickup_time). The PARTITION BY clause creates ~95K partitions (260 zones × 365 days), each requiring internal sorting. This is compute-intensive.

**Performance strategies discussed in SQL comments**:

| Strategy | Impact | How |
|---|---|---|
| Clustering key | High | `CLUSTER BY (pulocationid, tpep_pickup_datetime::DATE)` — aligns physical data layout with query's partition pattern |
| Search optimization | Medium | Helps filter specific zones or date ranges |
| Materialized view | High | Pre-compute LAG values, Snowflake maintains incrementally |
| Result caching | High (repeat runs) | Identical queries cached for 24h automatically |
| Warehouse sizing | Medium | Temporarily scale up for one-time heavy computation |

**Estimated runtime**: 60-120s on X-Small. With clustering key: 15-30s. With MEDIUM warehouse: 20-40s.

---

## Performance Principles Applied

These principles appear across all three queries and are relevant for interview discussion:

### 1. Aggregate Before Windowing
Reduce row count with GROUP BY before applying expensive window functions. Q1 reduces 35M → 3K before RANK(). Q2 reduces 35M → 24 before rolling average.

### 2. Predicate Pushdown via Range Filters
Use `col >= X AND col < Y` instead of `FUNCTION(col) = value`. The range predicate enables Snowflake to use micro-partition metadata (min/max values) to skip irrelevant partitions entirely. Wrapping the column in a function (like `YEAR()` or `DATE()`) prevents this optimization.

### 3. Snowflake Micro-Partition Awareness
Snowflake stores data in immutable micro-partitions (~16MB compressed). Each partition has metadata (min/max per column). Queries that align WHERE clauses with this metadata benefit from automatic partition pruning — reading only the partitions that contain relevant data.

### 4. Clustering Keys for Repeated Patterns
If a query pattern is repeated (e.g., always filtering by zone + date), a clustering key physically reorganises the table to match. This is a one-time cost that pays off across all future queries with that pattern.