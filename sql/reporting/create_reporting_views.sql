-- ============================================================================
-- MARKETMIND V1 - REPORTING VIEWS (CORRECTED FOR ACTUAL SCHEMA)
-- ============================================================================
-- Purpose: Create analytics views for reporting and Power BI dashboards
-- Database: marketmind_v1
-- Schema: gold
-- Created: 2026-04-26
-- Author: Sharique Mohammad
-- ============================================================================

-- ============================================================================
-- VIEW 1: DAILY PRICE SUMMARY
-- ============================================================================
-- Purpose: Daily price movements and volume by ticker
-- Use case: Market overview dashboard, price trend analysis

CREATE OR REPLACE VIEW gold.vw_daily_price_summary AS
SELECT 
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,
    ROUND(((close - open) / NULLIF(open, 0) * 100)::numeric, 2) AS daily_change_pct,
    ROUND((close - open)::numeric, 2) AS daily_change_abs,
    CASE 
        WHEN close > open THEN 'UP'
        WHEN close < open THEN 'DOWN'
        ELSE 'FLAT'
    END AS direction,
    CASE 
        WHEN close > open THEN 'GREEN'
        WHEN close < open THEN 'RED'
        ELSE 'GRAY'
    END AS color_indicator
FROM gold.ohlcv_bars
WHERE is_valid_ohlcv = true
ORDER BY ticker, date DESC;

COMMENT ON VIEW gold.vw_daily_price_summary IS 
'Daily price summary with change calculations for market overview dashboards';

-- ============================================================================
-- VIEW 2: TICKER PRICE TRENDS
-- ============================================================================
-- Purpose: Multi-day price trends with moving averages
-- Use case: Trend analysis, technical indicators visualization

CREATE OR REPLACE VIEW gold.vw_ticker_price_trends AS
SELECT 
    ticker,
    date,
    close,
    volume,
    AVG(close) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS sma_5,
    AVG(close) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20,
    MIN(close) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS min_20d,
    MAX(close) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS max_20d
FROM gold.ohlcv_bars
WHERE is_valid_ohlcv = true
ORDER BY ticker, date;

COMMENT ON VIEW gold.vw_ticker_price_trends IS 
'Price trends with 5-day and 20-day simple moving averages';

-- ============================================================================
-- VIEW 3: VOLUME ANALYSIS
-- ============================================================================
-- Purpose: Volume patterns and anomalies
-- Use case: Volume spike detection, liquidity analysis

CREATE OR REPLACE VIEW gold.vw_volume_analysis AS
SELECT 
    ticker,
    date,
    volume,
    AVG(volume) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS avg_volume_20d,
    ROUND(
        (volume::numeric / NULLIF(
            AVG(volume) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ), 0
        ))::numeric, 2
    ) AS volume_ratio,
    CASE 
        WHEN volume > 2 * AVG(volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) THEN 'HIGH_VOLUME_SPIKE'
        WHEN volume < 0.5 * AVG(volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) THEN 'LOW_VOLUME'
        ELSE 'NORMAL'
    END AS volume_category
FROM gold.ohlcv_bars
WHERE is_valid_ohlcv = true AND volume IS NOT NULL
ORDER BY ticker, date;

COMMENT ON VIEW gold.vw_volume_analysis IS 
'Volume analysis with 20-day average and spike detection';

-- ============================================================================
-- VIEW 4: CORPORATE ACTIONS TIMELINE
-- ============================================================================
-- Purpose: Corporate events timeline
-- Use case: Event-driven analysis, corporate calendar

CREATE OR REPLACE VIEW gold.vw_corporate_actions_timeline AS
SELECT 
    ticker,
    action_type,
    ex_dividend_date AS ex_date,
    cash_amount,
    declaration_date,
    record_date,
    payment_date,
    split_ratio,
    CASE 
        WHEN action_type = 'dividend' THEN 'Dividend Payment'
        WHEN action_type = 'split' THEN 'Stock Split'
        ELSE action_type
    END AS action_label
FROM gold.corporate_actions
ORDER BY ticker, ex_dividend_date DESC;

COMMENT ON VIEW gold.vw_corporate_actions_timeline IS 
'Corporate actions timeline for event analysis';

-- ============================================================================
-- VIEW 5: TICKER COVERAGE SUMMARY
-- ============================================================================
-- Purpose: Track ticker coverage and completeness
-- Use case: Coverage reporting, data gap analysis

CREATE OR REPLACE VIEW gold.vw_ticker_coverage AS
WITH date_range AS (
    SELECT 
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        MAX(date) - MIN(date) + 1 AS total_days
    FROM gold.ohlcv_bars
),
ticker_stats AS (
    SELECT 
        ticker,
        COUNT(*) AS records_count,
        MIN(date) AS first_date,
        MAX(date) AS last_date,
        MAX(date) - MIN(date) + 1 AS date_range_days
    FROM gold.ohlcv_bars
    WHERE is_valid_ohlcv = true
    GROUP BY ticker
)
SELECT 
    ts.ticker,
    ts.records_count,
    ts.first_date,
    ts.last_date,
    ts.date_range_days,
    dr.total_days AS expected_days,
    ROUND(
        (ts.records_count::numeric / NULLIF(dr.total_days, 0) * 100)::numeric, 2
    ) AS completeness_pct,
    CASE 
        WHEN ts.records_count::numeric / NULLIF(dr.total_days, 0) >= 0.95 THEN 'EXCELLENT'
        WHEN ts.records_count::numeric / NULLIF(dr.total_days, 0) >= 0.80 THEN 'GOOD'
        WHEN ts.records_count::numeric / NULLIF(dr.total_days, 0) >= 0.60 THEN 'FAIR'
        ELSE 'POOR'
    END AS coverage_rating
FROM ticker_stats ts
CROSS JOIN date_range dr
ORDER BY completeness_pct DESC;

COMMENT ON VIEW gold.vw_ticker_coverage IS 
'Ticker coverage and completeness metrics';

-- ============================================================================
-- VIEW 6: MACRO INDICATORS LATEST
-- ============================================================================
-- Purpose: Latest macroeconomic indicators
-- Use case: Economic dashboard, correlation analysis

CREATE OR REPLACE VIEW gold.vw_macro_indicators_latest AS
SELECT 
    indicator_name,
    date AS indicator_date,
    value AS indicator_value,
    unit,
    value_change,
    value_pct_change,
    indicator_category,
    ROW_NUMBER() OVER (PARTITION BY indicator_name ORDER BY date DESC) AS recency_rank
FROM gold.macro_indicators
ORDER BY indicator_name, date DESC;

COMMENT ON VIEW gold.vw_macro_indicators_latest IS 
'Latest macroeconomic indicators for economic dashboards';

-- ============================================================================
-- VIEW 7: SEC FILINGS RECENT
-- ============================================================================
-- Purpose: Recent SEC filings by ticker
-- Use case: Regulatory compliance dashboard, filing tracker

CREATE OR REPLACE VIEW gold.vw_sec_filings_recent AS
SELECT 
    ticker,
    form_type AS filing_type,
    filing_date,
    document_url AS filing_url,
    accession_number,
    company_name,
    is_periodic_report,
    is_amended,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY filing_date DESC) AS filing_rank
FROM gold.sec_filings
ORDER BY ticker, filing_date DESC;

COMMENT ON VIEW gold.vw_sec_filings_recent IS 
'Recent SEC filings tracker for compliance monitoring';

-- ============================================================================
-- VIEW 8: DATA FRESHNESS REPORT
-- ============================================================================
-- Purpose: Track data currency by ticker
-- Use case: SLA monitoring, data freshness tracking

CREATE OR REPLACE VIEW gold.vw_data_freshness AS
SELECT 
    ticker,
    MAX(date) AS latest_data_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT date) AS trading_days_count,
    CASE 
        WHEN CURRENT_DATE - MAX(date) <= 7 THEN 'FRESH'
        WHEN CURRENT_DATE - MAX(date) <= 30 THEN 'STALE'
        ELSE 'VERY_STALE'
    END AS freshness_status,
    CURRENT_DATE - MAX(date) AS days_since_last_update
FROM gold.ohlcv_bars
WHERE is_valid_ohlcv = true
GROUP BY ticker
ORDER BY latest_data_date DESC;

COMMENT ON VIEW gold.vw_data_freshness IS 
'Data freshness tracking for SLA monitoring';

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Test each view
SELECT 'vw_daily_price_summary' AS view_name, COUNT(*) AS row_count FROM gold.vw_daily_price_summary;
SELECT 'vw_ticker_price_trends' AS view_name, COUNT(*) AS row_count FROM gold.vw_ticker_price_trends;
SELECT 'vw_volume_analysis' AS view_name, COUNT(*) AS row_count FROM gold.vw_volume_analysis;
SELECT 'vw_corporate_actions_timeline' AS view_name, COUNT(*) AS row_count FROM gold.vw_corporate_actions_timeline;
SELECT 'vw_ticker_coverage' AS view_name, COUNT(*) AS row_count FROM gold.vw_ticker_coverage;
SELECT 'vw_macro_indicators_latest' AS view_name, COUNT(*) AS row_count FROM gold.vw_macro_indicators_latest;
SELECT 'vw_sec_filings_recent' AS view_name, COUNT(*) AS row_count FROM gold.vw_sec_filings_recent;
SELECT 'vw_data_freshness' AS view_name, COUNT(*) AS row_count FROM gold.vw_data_freshness;

-- ============================================================================
-- END OF REPORTING VIEWS
-- ============================================================================
