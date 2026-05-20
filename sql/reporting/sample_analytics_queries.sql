-- ============================================================================
-- MARKETMIND V1 - SAMPLE ANALYTICS QUERIES
-- ============================================================================
-- Purpose: Ready-to-run queries for screenshots and portfolio demonstrations
-- Database: marketmind_v1
-- Schema: gold
-- Created: 2026-04-26
-- Author: Sharique Mohammad
-- ============================================================================

-- ============================================================================
-- QUERY 1: TOP PERFORMERS THIS WEEK
-- ============================================================================
-- Shows best performing stocks by percentage gain
-- Use for: Market overview dashboard screenshot

SELECT 
    ticker,
    MIN(date) AS period_start,
    MAX(date) AS period_end,
    MIN(close) AS starting_price,
    MAX(close) AS ending_price,
    ROUND(((MAX(close) - MIN(close)) / NULLIF(MIN(close), 0) * 100)::numeric, 2) AS gain_pct,
    SUM(volume) AS total_volume
FROM gold.vw_daily_price_summary
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY ticker
ORDER BY gain_pct DESC;

-- ============================================================================
-- QUERY 2: DAILY PRICE MOVEMENTS
-- ============================================================================
-- Shows recent daily movements with color indicators
-- Use for: Price trend screenshot

SELECT 
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,
    daily_change_pct,
    direction,
    color_indicator
FROM gold.vw_daily_price_summary
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date DESC, ticker;

-- ============================================================================
-- QUERY 3: VOLUME SPIKES DETECTED
-- ============================================================================
-- Shows unusual volume activity
-- Use for: Anomaly detection capability screenshot

SELECT 
    ticker,
    date,
    volume,
    avg_volume_20d,
    volume_ratio,
    volume_category
FROM gold.vw_volume_analysis
WHERE volume_category = 'HIGH_VOLUME_SPIKE'
ORDER BY volume_ratio DESC
LIMIT 20;

-- ============================================================================
-- QUERY 4: PRICE TRENDS WITH MOVING AVERAGES
-- ============================================================================
-- Shows technical indicators calculation
-- Use for: Analytics capability screenshot

SELECT 
    ticker,
    date,
    close,
    ROUND(sma_5::numeric, 2) AS sma_5_day,
    ROUND(sma_20::numeric, 2) AS sma_20_day,
    CASE 
        WHEN sma_5 > sma_20 THEN 'BULLISH'
        WHEN sma_5 < sma_20 THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS trend_signal
FROM gold.vw_ticker_price_trends
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY ticker, date DESC;

-- ============================================================================
-- QUERY 5: DATA QUALITY SUMMARY
-- ============================================================================
-- Shows quality framework in action
-- Use for: Data governance screenshot

SELECT 
    alert_type,
    severity,
    alert_count,
    affected_tickers,
    ticker_list
FROM gold.vw_data_quality_metrics
ORDER BY 
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
    END,
    alert_count DESC;

-- ============================================================================
-- QUERY 6: PIPELINE EXECUTION HEALTH
-- ============================================================================
-- Shows operational reliability
-- Use for: Production reliability screenshot

SELECT 
    dag_id,
    task_id,
    execution_count,
    success_count,
    failed_count,
    success_rate_pct,
    ROUND(EXTRACT(EPOCH FROM avg_execution_time)::numeric, 2) AS avg_exec_seconds,
    last_run
FROM gold.vw_pipeline_health
ORDER BY dag_id, task_id;

-- ============================================================================
-- QUERY 7: DATA FRESHNESS CHECK
-- ============================================================================
-- Shows data currency monitoring
-- Use for: SLA monitoring screenshot

SELECT 
    ticker,
    latest_data_date,
    last_loaded_at,
    total_records,
    trading_days_count,
    time_since_update,
    freshness_status
FROM gold.vw_data_freshness
ORDER BY freshness_status, last_loaded_at DESC;

-- ============================================================================
-- QUERY 8: TICKER COVERAGE REPORT
-- ============================================================================
-- Shows data completeness tracking
-- Use for: Coverage metrics screenshot

SELECT 
    ticker,
    records_count,
    first_date,
    last_date,
    expected_days,
    completeness_pct,
    coverage_rating
FROM gold.vw_ticker_coverage
ORDER BY completeness_pct DESC;

-- ============================================================================
-- QUERY 9: CORPORATE ACTIONS CALENDAR
-- ============================================================================
-- Shows event-driven data capability
-- Use for: Corporate events screenshot

SELECT 
    ticker,
    action_label AS event_type,
    ex_date,
    cash_amount,
    pay_date
FROM gold.vw_corporate_actions_timeline
WHERE ex_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY ex_date DESC, ticker;

-- ============================================================================
-- QUERY 10: RECENT SEC FILINGS
-- ============================================================================
-- Shows regulatory data integration
-- Use for: Multi-source data screenshot

SELECT 
    ticker,
    filing_type,
    filing_date,
    filing_url,
    filing_rank
FROM gold.vw_sec_filings_recent
WHERE filing_rank <= 5
ORDER BY ticker, filing_date DESC;

-- ============================================================================
-- QUERY 11: MACRO INDICATORS DASHBOARD
-- ============================================================================
-- Shows economic data integration
-- Use for: Macro analysis screenshot

SELECT 
    indicator_name,
    indicator_date,
    indicator_value,
    unit
FROM gold.vw_macro_indicators_latest
WHERE recency_rank = 1
ORDER BY indicator_name;

-- ============================================================================
-- QUERY 12: PRICE & VOLUME CORRELATION
-- ============================================================================
-- Shows analytical depth
-- Use for: Advanced analytics screenshot

SELECT 
    ticker,
    date,
    close,
    daily_change_pct,
    volume,
    volume_ratio,
    CASE 
        WHEN daily_change_pct > 2 AND volume_ratio > 1.5 THEN 'STRONG_BUY_SIGNAL'
        WHEN daily_change_pct < -2 AND volume_ratio > 1.5 THEN 'STRONG_SELL_SIGNAL'
        WHEN ABS(daily_change_pct) > 1 AND volume_ratio > 1.2 THEN 'MODERATE_SIGNAL'
        ELSE 'NORMAL_TRADING'
    END AS trading_signal
FROM gold.vw_daily_price_summary dps
JOIN gold.vw_volume_analysis va USING (ticker, date)
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
  AND ABS(daily_change_pct) > 1
ORDER BY ABS(daily_change_pct) DESC, volume_ratio DESC
LIMIT 50;

-- ============================================================================
-- QUERY 13: PIPELINE PERFORMANCE METRICS
-- ============================================================================
-- Shows system throughput
-- Use for: Performance metrics screenshot

SELECT 
    DATE(start_time) AS execution_date,
    dag_id,
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE status = 'success') AS successful_runs,
    AVG(EXTRACT(EPOCH FROM execution_time)) AS avg_duration_seconds,
    MIN(EXTRACT(EPOCH FROM execution_time)) AS min_duration_seconds,
    MAX(EXTRACT(EPOCH FROM execution_time)) AS max_duration_seconds
FROM gold.pipeline_audit
WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(start_time), dag_id
ORDER BY execution_date DESC, dag_id;

-- ============================================================================
-- QUERY 14: DATA LINEAGE TRACE
-- ============================================================================
-- Shows end-to-end traceability
-- Use for: Data lineage screenshot

SELECT 
    ob.ticker,
    ob.date,
    ob.close,
    ob.created_at AS bronze_loaded,
    pa.start_time AS pipeline_start,
    pa.end_time AS pipeline_end,
    pa.status AS pipeline_status,
    EXTRACT(EPOCH FROM (pa.end_time - pa.start_time)) AS processing_seconds
FROM gold.ohlcv_bars ob
LEFT JOIN gold.pipeline_audit pa 
    ON DATE(pa.start_time) = ob.date
    AND pa.dag_id = 'daily_market_data'
WHERE ob.date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY ob.date DESC, ob.ticker;

-- ============================================================================
-- QUERY 15: COMPREHENSIVE SYSTEM HEALTH
-- ============================================================================
-- Single query showing overall system status
-- Use for: Executive dashboard screenshot

SELECT 
    'Total Tickers' AS metric,
    COUNT(DISTINCT ticker)::text AS value
FROM gold.ohlcv_bars

UNION ALL

SELECT 
    'Total Records',
    COUNT(*)::text
FROM gold.ohlcv_bars

UNION ALL

SELECT 
    'Date Range',
    MIN(date)::text || ' to ' || MAX(date)::text
FROM gold.ohlcv_bars

UNION ALL

SELECT 
    'Avg Completeness',
    ROUND(AVG(completeness_pct), 2)::text || '%'
FROM gold.vw_ticker_coverage

UNION ALL

SELECT 
    'Pipeline Success Rate',
    ROUND(AVG(success_rate_pct), 2)::text || '%'
FROM gold.vw_pipeline_health

UNION ALL

SELECT 
    'Fresh Data Tickers',
    COUNT(*)::text
FROM gold.vw_data_freshness
WHERE freshness_status = 'FRESH'

UNION ALL

SELECT 
    'Quality Alerts',
    COUNT(*)::text
FROM gold.quality_alerts

UNION ALL

SELECT 
    'Corporate Actions',
    COUNT(*)::text
FROM gold.corporate_actions

UNION ALL

SELECT 
    'SEC Filings',
    COUNT(*)::text
FROM gold.filings_metadata

UNION ALL

SELECT 
    'Macro Indicators',
    COUNT(DISTINCT indicator_name)::text
FROM gold.macro_indicators;

-- ============================================================================
-- END OF SAMPLE ANALYTICS QUERIES
-- ============================================================================
