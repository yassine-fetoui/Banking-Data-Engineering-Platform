-- =============================================================================
-- models/silver/fct_transactions.sql
-- Incremental Silver fact table: cleansed, deduplicated, FX-converted
-- =============================================================================

{{
  config(
    materialized        = "incremental",
    unique_key          = "transaction_id",
    on_schema_change    = "append_new_columns",
    partition_by        = {"field": "transaction_date", "data_type": "date"},
    cluster_by          = ["customer_id", "transaction_type"],
    incremental_strategy = "merge"
  )
}}

WITH raw_txn AS (
    SELECT *
    FROM {{ source('bronze', 'transactions') }}
    {% if is_incremental() %}
        -- Only process records newer than the last run
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    -- Deduplicate: keep the most recently ingested record per transaction_id
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY _ingested_at DESC
            ) AS _rn
        FROM raw_txn
    )
    WHERE _rn = 1
),

with_fx AS (
    -- Convert all amounts to AED base currency for consolidated reporting
    SELECT
        t.*,
        COALESCE(fx.rate_to_aed, 1.0)                               AS fx_rate,
        ROUND(t.amount * COALESCE(fx.rate_to_aed, 1.0), 4)         AS amount_aed
    FROM deduplicated t
    LEFT JOIN {{ ref('stg_fx_rates') }} fx
        ON  t.currency   = fx.from_currency
        AND DATE(t.transaction_datetime) = fx.rate_date
),

enriched AS (
    SELECT
        w.transaction_id,
        w.account_id,
        w.customer_id,
        w.transaction_datetime,
        DATE(w.transaction_datetime)                                  AS transaction_date,
        EXTRACT(HOUR FROM w.transaction_datetime)                     AS transaction_hour,
        w.amount,
        w.currency,
        w.amount_aed,
        w.fx_rate,
        w.transaction_type,
        w.channel,
        w.merchant_id,
        w.merchant_category,
        mc.merchant_category_group,                                   -- Enriched from reference
        w.country_code,
        c.region                                                      AS country_region,
        w.status,
        w.reference_id,
        -- AML flags computed at Silver (fast rule-based, not ML)
        w.amount_aed > {{ var('ctr_threshold') }}                     AS exceeds_ctr_threshold,
        w.amount_aed > 0 AND w.amount_aed % 1000 = 0                 AS is_round_amount,
        w.country_code NOT IN (
            SELECT country_code FROM {{ ref('seed_approved_countries') }}
        )                                                             AS is_high_risk_jurisdiction,
        -- Audit
        w._source_system,
        w._batch_id,
        w._ingested_at,
        {{ audit_columns() }}
    FROM with_fx w
    LEFT JOIN {{ ref('seed_merchant_categories') }}   mc ON w.merchant_category = mc.merchant_category_code
    LEFT JOIN {{ ref('seed_country_reference') }}     c  ON w.country_code = c.country_code
    -- Exclude corrupt / unprocessable records
    WHERE w.transaction_id IS NOT NULL
      AND w.account_id     IS NOT NULL
      AND w.amount         IS NOT NULL
      AND w.amount         > 0
)

SELECT * FROM enriched
