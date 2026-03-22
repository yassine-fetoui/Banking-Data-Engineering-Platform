-- =============================================================================
-- macros/generate_schema_name.sql
-- Override dbt's default schema naming to use env-prefixed schemas
-- e.g., bronze → banking_dev_bronze, banking_prod_bronze
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


-- =============================================================================
-- macros/hash_pii.sql
-- Deterministic PII tokenisation with environment-specific salt
-- Usage: {{ hash_pii('national_id') }}
-- =============================================================================

{% macro hash_pii(column_name) %}
    SHA2(CONCAT(COALESCE({{ column_name }}::VARCHAR, ''), '{{ var("pii_salt") }}'), 256)
{% endmacro %}


-- =============================================================================
-- macros/is_incremental_date.sql
-- Reusable incremental filter — used in all Silver incremental models
-- =============================================================================

{% macro incremental_date_filter(timestamp_col) %}
    {% if is_incremental() %}
        WHERE {{ timestamp_col }} > (SELECT MAX({{ timestamp_col }}) FROM {{ this }})
    {% endif %}
{% endmacro %}


-- =============================================================================
-- macros/assert_row_count.sql
-- Custom test macro: assert a model has at least N rows
-- Usage in schema.yml: - dbt_utils.assert_row_count: {min_value: 1000}
-- =============================================================================

{% macro test_assert_row_count(model, column_name, min_value=1) %}
    SELECT COUNT(*) AS actual_count
    FROM {{ model }}
    HAVING COUNT(*) < {{ min_value }}
{% endmacro %}


-- =============================================================================
-- macros/audit_columns.sql
-- Add standard audit columns to any model
-- =============================================================================

{% macro audit_columns() %}
    CURRENT_TIMESTAMP                   AS _dbt_updated_at,
    '{{ invocation_id }}'               AS _dbt_invocation_id,
    '{{ run_started_at }}'              AS _dbt_run_started_at
{% endmacro %}
