-- =============================================================================
-- snapshots/snap_customers_scd2.sql
-- SCD Type 2 snapshot: tracks full KYC history for every customer.
-- dbt handles valid_from, valid_to, dbt_scd_id, dbt_is_current automatically.
-- =============================================================================

{% snapshot snap_customers_scd2 %}

{{
  config(
    target_schema = "silver",
    unique_key    = "customer_id",
    strategy      = "check",
    check_cols    = ["kyc_status", "risk_rating", "customer_segment", "relationship_manager"],
    updated_at    = "last_updated",
    invalidate_hard_deletes = true
  )
}}

SELECT
    customer_id,
    -- PII: tokenised at ingestion, only hash lands in Silver
    full_name_hashed,
    national_id_hashed,
    date_of_birth,
    nationality,
    risk_rating,
    kyc_status,
    kyc_review_date,
    customer_segment,
    relationship_manager,
    onboarding_date,
    last_updated,
    _source_system,
    _batch_id
FROM {{ source('bronze', 'customers') }}

{% endsnapshot %}
