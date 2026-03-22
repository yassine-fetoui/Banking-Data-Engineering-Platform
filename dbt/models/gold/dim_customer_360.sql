-- =============================================================================
-- models/gold/dim_customer_360.sql
-- 360° customer view: current KYC state + behavioural summary + risk scores
-- Materialized as table — optimised for BI dashboards and ML feature serving
-- =============================================================================

{{
  config(
    materialized = "table",
    sort         = ["risk_band", "customer_segment"],
    dist         = "customer_id"
  )
}}

WITH current_kyc AS (
    -- Current KYC record from SCD2 snapshot (dbt_is_current = true)
    SELECT *
    FROM {{ ref('snap_customers_scd2') }}
    WHERE dbt_is_current = true
),

transaction_summary AS (
    SELECT
        customer_id,
        COUNT(*)                                        AS total_txn_count,
        SUM(amount)                                     AS total_txn_volume,
        AVG(amount)                                     AS avg_txn_amount,
        MAX(transaction_datetime)                       AS last_transaction_at,
        MIN(transaction_datetime)                       AS first_transaction_at,
        COUNT(DISTINCT merchant_category)               AS distinct_merchant_categories,
        COUNT(DISTINCT country_code)                    AS distinct_countries,
        COUNTIF(transaction_type = 'INTERNATIONAL')     AS intl_txn_count,
        COUNTIF(channel = 'ATM')                        AS atm_txn_count
    FROM {{ ref('fct_transactions') }}
    WHERE status = 'COMPLETED'
    GROUP BY 1
),

aml_scores AS (
    SELECT
        customer_id,
        composite_risk_score,
        risk_band,
        structuring_flag,
        new_country_flag,
        txn_count_7d,
        txn_volume_30d
    FROM {{ ref('aml_risk_scores') }}
    WHERE score_date = CURRENT_DATE
),

account_summary AS (
    SELECT
        customer_id,
        COUNT(*)                                        AS account_count,
        SUM(current_balance)                            AS total_balance_aed,
        ARRAY_AGG(DISTINCT product_type)                AS product_types,
        MAX(CASE WHEN account_status = 'DORMANT' THEN 1 ELSE 0 END) AS has_dormant_account
    FROM {{ ref('dim_accounts') }}
    WHERE account_status != 'CLOSED'
    GROUP BY 1
)

SELECT
    k.customer_id,
    k.nationality,
    k.customer_segment,
    k.risk_rating                                       AS kyc_risk_rating,
    k.kyc_status,
    k.kyc_review_date,
    k.relationship_manager,
    k.onboarding_date,
    -- Transaction behaviour
    COALESCE(t.total_txn_count, 0)                      AS total_txn_count,
    COALESCE(t.total_txn_volume, 0)                     AS total_txn_volume_aed,
    COALESCE(t.avg_txn_amount, 0)                       AS avg_txn_amount_aed,
    t.last_transaction_at,
    t.first_transaction_at,
    COALESCE(t.distinct_countries, 0)                   AS distinct_countries,
    COALESCE(t.intl_txn_count, 0)                       AS intl_txn_count,
    -- Account summary
    COALESCE(a.account_count, 0)                        AS account_count,
    COALESCE(a.total_balance_aed, 0)                    AS total_balance_aed,
    a.product_types,
    COALESCE(a.has_dormant_account, 0) = 1              AS has_dormant_account,
    -- AML / Risk
    COALESCE(r.composite_risk_score, 0)                 AS aml_risk_score,
    COALESCE(r.risk_band, 'LOW')                        AS aml_risk_band,
    COALESCE(r.structuring_flag, false)                 AS aml_structuring_flag,
    COALESCE(r.new_country_flag, false)                 AS aml_new_country_flag,
    -- Tenure
    DATEDIFF(CURRENT_DATE, k.onboarding_date)           AS tenure_days,
    -- Dormancy: no transactions in 90 days
    DATEDIFF(CURRENT_DATE, t.last_transaction_at) > 90  AS is_dormant,
    -- Audit
    CURRENT_TIMESTAMP                                   AS _refreshed_at
FROM current_kyc          k
LEFT JOIN transaction_summary  t  ON k.customer_id = t.customer_id
LEFT JOIN aml_scores           r  ON k.customer_id = r.customer_id
LEFT JOIN account_summary      a  ON k.customer_id = a.customer_id


-- =============================================================================
-- models/gold/fct_daily_pnl.sql
-- Daily P&L and balance sheet per product / currency
-- =============================================================================

-- (separate file in production — combined here for readability)

/*
{{
  config(
    materialized = "incremental",
    unique_key   = ["report_date", "product_type", "currency"],
    on_schema_change = "append_new_columns"
  )
}}

WITH daily_balances AS (
    SELECT
        CURRENT_DATE                                    AS report_date,
        product_type,
        currency,
        SUM(current_balance)                            AS total_balance,
        COUNT(DISTINCT account_id)                      AS account_count,
        SUM(CASE WHEN account_status = 'ACTIVE' THEN current_balance ELSE 0 END) AS active_balance
    FROM {{ ref('dim_accounts') }}
    GROUP BY 1, 2, 3
),

daily_txn_revenue AS (
    SELECT
        DATE(transaction_datetime)                      AS report_date,
        product_type,
        currency,
        SUM(fee_amount)                                 AS total_fees,
        SUM(interest_income)                            AS total_interest,
        COUNT(*)                                        AS txn_count,
        SUM(amount)                                     AS txn_volume
    FROM {{ ref('fct_transactions') }}
    {% if is_incremental() %}
        WHERE DATE(transaction_datetime) = CURRENT_DATE
    {% endif %}
    GROUP BY 1, 2, 3
)

SELECT
    b.report_date,
    b.product_type,
    b.currency,
    b.total_balance,
    b.account_count,
    b.active_balance,
    COALESCE(r.total_fees, 0)       AS total_fee_income,
    COALESCE(r.total_interest, 0)   AS total_interest_income,
    COALESCE(r.txn_count, 0)        AS daily_txn_count,
    COALESCE(r.txn_volume, 0)       AS daily_txn_volume,
    CURRENT_TIMESTAMP               AS _refreshed_at
FROM daily_balances b
LEFT JOIN daily_txn_revenue r
    ON  b.report_date   = r.report_date
    AND b.product_type  = r.product_type
    AND b.currency      = r.currency
*/


-- =============================================================================
-- models/gold/rpt_regulatory_lcr.sql
-- Liquidity Coverage Ratio (LCR) — Basel III regulatory report
-- =============================================================================

/*
{{
  config(
    materialized = "table",
    tags         = ["regulatory", "basel3", "daily"]
  )
}}

-- LCR = High-Quality Liquid Assets (HQLA) / Net Cash Outflows (30 days)
-- Regulatory minimum: 100%

WITH hqla AS (
    -- Level 1: Cash + Central bank reserves + Sovereign bonds (0% haircut)
    -- Level 2A: Agency bonds, covered bonds (15% haircut)
    -- Level 2B: Corporate bonds BBB+ (50% haircut)
    SELECT
        asset_category,
        SUM(market_value * (1 - haircut_pct))           AS hqla_value_aed
    FROM {{ ref('dim_treasury_assets') }}
    WHERE asset_class IN ('L1_ASSETS', 'L2A_ASSETS', 'L2B_ASSETS')
      AND report_date = CURRENT_DATE
    GROUP BY 1
),

net_cash_outflows AS (
    SELECT
        SUM(outflow_amount * run_off_rate)              AS gross_outflows,
        SUM(inflow_amount * inflow_cap_rate)            AS capped_inflows
    FROM {{ ref('fct_cash_flows_30d') }}
    WHERE report_date = CURRENT_DATE
)

SELECT
    CURRENT_DATE                                        AS report_date,
    SUM(h.hqla_value_aed)                               AS total_hqla_aed,
    n.gross_outflows,
    n.capped_inflows,
    n.gross_outflows - n.capped_inflows                 AS net_cash_outflows,
    ROUND(
        SUM(h.hqla_value_aed) / NULLIF(n.gross_outflows - n.capped_inflows, 0) * 100, 2
    )                                                   AS lcr_pct,
    CASE
        WHEN SUM(h.hqla_value_aed) / NULLIF(n.gross_outflows - n.capped_inflows, 0) >= 1.0
        THEN 'COMPLIANT'
        ELSE 'BREACH'
    END                                                 AS lcr_status,
    CURRENT_TIMESTAMP                                   AS _generated_at
FROM hqla h
CROSS JOIN net_cash_outflows n
GROUP BY 2, 3, 4, 5, 6
*/
