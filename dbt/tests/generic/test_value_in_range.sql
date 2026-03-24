-- =============================================================================
-- Generic test: assert column values fall within a configurable range
-- =============================================================================
-- Parameters:
--   model:     the model being tested (auto-injected by DBT)
--   column_name: the column to check (auto-injected by DBT)
--   min_value: minimum allowed value (inclusive)
--   max_value: maximum allowed value (inclusive)
--
-- Returns rows that violate the range constraint.
-- =============================================================================

{% test value_in_range(model, column_name, min_value, max_value) %}

select
    {{ column_name }},
    count(*) as violation_count
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}
group by {{ column_name }}

{% endtest %}