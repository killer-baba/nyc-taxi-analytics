-- =============================================================================
-- Custom schema name macro
-- =============================================================================
-- Overrides DBT's default behavior of prepending the target schema.
-- Default DBT: target_schema + "_" + custom_schema = "RAW_staging"
-- This macro: if a custom schema is specified, use it directly = "STAGING"
--
-- This is the standard production pattern. Nearly every real DBT project
-- overrides this macro.
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}