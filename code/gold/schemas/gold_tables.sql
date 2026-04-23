-- ====================================================================
-- Gold Layer PostgreSQL Schema
-- MarketMind Intelligence Platform V1
-- Author: Sharique Mohammad
-- Date: April 21, 2026
-- ====================================================================
-- FILE: code/gold/schemas/gold_tables.sql
-- Purpose: Create Gold layer tables in PostgreSQL
-- ====================================================================

-- Create gold schema if not exists
CREATE SCHEMA IF NOT EXISTS gold;

-- ====================================================================
-- Table: gold.ohlcv_bars
-- Purpose: Clean OHLCV market data
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.ohlcv_bars (
    ticker VARCHAR(10) NOT NULL,
    timestamp BIGINT NOT NULL,
    granularity VARCHAR(20) NOT NULL,
    open DECIMAL(15, 4) NOT NULL,
    high DECIMAL(15, 4) NOT NULL,
    low DECIMAL(15, 4) NOT NULL,
    close DECIMAL(15, 4) NOT NULL,
    volume BIGINT,
    vwap DECIMAL(15, 4),
    trade_count INTEGER,
    adjusted BOOLEAN DEFAULT TRUE,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    is_trading_day BOOLEAN DEFAULT TRUE,
    is_valid_ohlcv BOOLEAN DEFAULT TRUE,
    
    -- Primary key
    PRIMARY KEY (ticker, timestamp, granularity),
    
    -- Check constraints
    CONSTRAINT chk_ohlcv_high_low CHECK (high >= low),
    CONSTRAINT chk_ohlcv_open_range CHECK (open >= low AND open <= high),
    CONSTRAINT chk_ohlcv_close_range CHECK (close >= low AND close <= high),
    CONSTRAINT chk_ohlcv_volume CHECK (volume >= 0),
    CONSTRAINT chk_ohlcv_prices CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_date ON gold.ohlcv_bars(ticker, date);
CREATE INDEX IF NOT EXISTS idx_ohlcv_date ON gold.ohlcv_bars(date);
CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker ON gold.ohlcv_bars(ticker);

-- ====================================================================
-- Table: gold.corporate_actions
-- Purpose: Stock splits and dividend data
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.corporate_actions (
    ticker VARCHAR(10) NOT NULL,
    action_type VARCHAR(20) NOT NULL,
    execution_date DATE,
    split_ratio DECIMAL(10, 6),
    ex_dividend_date DATE,
    payment_date DATE,
    record_date DATE,
    cash_amount DECIMAL(10, 4),
    declaration_date DATE,
    frequency INTEGER,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    is_forward_split BOOLEAN,
    is_reverse_split BOOLEAN,
    
    -- Check constraints
    CONSTRAINT chk_action_type CHECK (action_type IN ('SPLIT', 'DIVIDEND')),
    CONSTRAINT chk_split_ratio CHECK (split_ratio IS NULL OR split_ratio > 0),
    CONSTRAINT chk_cash_amount CHECK (cash_amount IS NULL OR cash_amount > 0)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_corp_actions_ticker ON gold.corporate_actions(ticker);
CREATE INDEX IF NOT EXISTS idx_corp_actions_type ON gold.corporate_actions(action_type);
CREATE INDEX IF NOT EXISTS idx_corp_actions_exec_date ON gold.corporate_actions(execution_date);
CREATE INDEX IF NOT EXISTS idx_corp_actions_ex_div_date ON gold.corporate_actions(ex_dividend_date);

-- ====================================================================
-- Table: gold.macro_indicators
-- Purpose: Macroeconomic indicators
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.macro_indicators (
    indicator_name VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(15, 4),
    unit VARCHAR(50),
    frequency VARCHAR(20),
    forecast_value DECIMAL(15, 4),
    previous_value DECIMAL(15, 4),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER,
    value_change DECIMAL(15, 4),
    value_pct_change DECIMAL(10, 2),
    indicator_category VARCHAR(50),
    source_url TEXT,
    
    -- Primary key
    PRIMARY KEY (indicator_name, date),
    
    -- Check constraints
    CONSTRAINT chk_frequency CHECK (frequency IN ('DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'ANNUALLY'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_macro_indicator_name ON gold.macro_indicators(indicator_name);
CREATE INDEX IF NOT EXISTS idx_macro_date ON gold.macro_indicators(date);
CREATE INDEX IF NOT EXISTS idx_macro_category ON gold.macro_indicators(indicator_category);

-- ====================================================================
-- Table: gold.sec_filings
-- Purpose: SEC filing metadata
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.sec_filings (
    accession_number VARCHAR(30) PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    cik VARCHAR(10) NOT NULL,
    company_name VARCHAR(200),
    form_type VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    report_date DATE,
    filing_year INTEGER NOT NULL,
    filing_quarter INTEGER,
    filing_category VARCHAR(50),
    is_periodic_report BOOLEAN DEFAULT FALSE,
    is_amended BOOLEAN DEFAULT FALSE,
    is_xbrl BOOLEAN DEFAULT FALSE,
    is_inline_xbrl BOOLEAN DEFAULT FALSE,
    has_structured_data BOOLEAN DEFAULT FALSE,
    filing_lag_days INTEGER,
    document_url TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_filings_ticker ON gold.sec_filings(ticker);
CREATE INDEX IF NOT EXISTS idx_filings_form_type ON gold.sec_filings(form_type);
CREATE INDEX IF NOT EXISTS idx_filings_filing_date ON gold.sec_filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_filings_category ON gold.sec_filings(filing_category);

-- ====================================================================
-- Table: gold.pipeline_audit
-- Purpose: Pipeline execution metrics
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.pipeline_audit (
    audit_id VARCHAR(50) PRIMARY KEY,
    connector VARCHAR(50) NOT NULL,
    execution_mode VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP NOT NULL,
    duration_seconds DECIMAL(10, 2),
    duration_minutes DECIMAL(10, 2),
    records_retrieved INTEGER,
    records_written INTEGER,
    bytes_written BIGINT,
    megabytes_written DECIMAL(10, 2),
    api_calls_made INTEGER,
    rate_limited BOOLEAN DEFAULT FALSE,
    write_success_rate DECIMAL(5, 2),
    records_per_second DECIMAL(10, 2),
    records_per_api_call DECIMAL(10, 2),
    is_success BOOLEAN,
    is_failure BOOLEAN,
    is_slow BOOLEAN,
    error_message TEXT,
    
    -- Check constraints
    CONSTRAINT chk_audit_status CHECK (status IN ('SUCCESS', 'FAILURE', 'PARTIAL_SUCCESS', 'SKIPPED')),
    CONSTRAINT chk_audit_mode CHECK (execution_mode IN ('BATCH', 'INCREMENTAL', 'BACKFILL'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_audit_connector ON gold.pipeline_audit(connector);
CREATE INDEX IF NOT EXISTS idx_audit_start_datetime ON gold.pipeline_audit(start_datetime);
CREATE INDEX IF NOT EXISTS idx_audit_status ON gold.pipeline_audit(status);

-- ====================================================================
-- Table: gold.quality_alerts
-- Purpose: Data quality alerts
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.quality_alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    layer VARCHAR(20) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    check_result VARCHAR(20) NOT NULL,
    check_datetime TIMESTAMP NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolution_datetime TIMESTAMP,
    time_to_resolution_hours DECIMAL(10, 2),
    failure_description TEXT,
    row_count_checked INTEGER,
    failure_count INTEGER,
    failure_rate DECIMAL(5, 2),
    threshold_value DECIMAL(10, 2),
    actual_value DECIMAL(10, 2),
    pipeline_blocked BOOLEAN DEFAULT FALSE,
    severity_score INTEGER,
    impact_score DECIMAL(10, 2),
    is_critical BOOLEAN,
    is_unresolved BOOLEAN,
    
    -- Check constraints
    CONSTRAINT chk_alert_layer CHECK (layer IN ('BRONZE', 'SILVER', 'GOLD')),
    CONSTRAINT chk_alert_check_type CHECK (check_type IN ('COMPLETENESS', 'FRESHNESS', 'UNIQUENESS', 'VALIDITY', 'CONSISTENCY', 'SCHEMA')),
    CONSTRAINT chk_alert_severity CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    CONSTRAINT chk_alert_result CHECK (check_result IN ('PASS', 'FAIL'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_alerts_layer ON gold.quality_alerts(layer);
CREATE INDEX IF NOT EXISTS idx_alerts_table ON gold.quality_alerts(table_name);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON gold.quality_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON gold.quality_alerts(resolved);
CREATE INDEX IF NOT EXISTS idx_alerts_datetime ON gold.quality_alerts(check_datetime);

-- ====================================================================
-- Views and Aggregations
-- ====================================================================

-- Daily market summary view
CREATE OR REPLACE VIEW gold.v_daily_market_summary AS
SELECT 
    date,
    COUNT(DISTINCT ticker) as ticker_count,
    SUM(volume) as total_volume,
    AVG(close) as avg_close,
    MAX(high) as max_high,
    MIN(low) as min_low
FROM gold.ohlcv_bars
WHERE granularity = 'daily'
GROUP BY date
ORDER BY date DESC;

-- Quality dashboard view
CREATE OR REPLACE VIEW gold.v_quality_dashboard AS
SELECT 
    layer,
    table_name,
    check_type,
    severity,
    COUNT(*) as alert_count,
    SUM(CASE WHEN resolved THEN 1 ELSE 0 END) as resolved_count,
    SUM(CASE WHEN resolved THEN 0 ELSE 1 END) as unresolved_count,
    AVG(failure_rate) as avg_failure_rate
FROM gold.quality_alerts
GROUP BY layer, table_name, check_type, severity
ORDER BY severity DESC, avg_failure_rate DESC;

-- Pipeline performance view
CREATE OR REPLACE VIEW gold.v_pipeline_performance AS
SELECT 
    connector,
    execution_mode,
    DATE(start_datetime) as execution_date,
    COUNT(*) as execution_count,
    SUM(CASE WHEN is_success THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN is_failure THEN 1 ELSE 0 END) as failure_count,
    AVG(duration_minutes) as avg_duration_minutes,
    SUM(records_written) as total_records_written
FROM gold.pipeline_audit
GROUP BY connector, execution_mode, DATE(start_datetime)
ORDER BY execution_date DESC, connector;

-- Comments
COMMENT ON SCHEMA gold IS 'Gold layer - Business-ready analytics tables';
COMMENT ON TABLE gold.ohlcv_bars IS 'Clean OHLCV market data with quality checks applied';
COMMENT ON TABLE gold.corporate_actions IS 'Stock splits and dividend corporate actions';
COMMENT ON TABLE gold.macro_indicators IS 'Macroeconomic indicators with derived metrics';
COMMENT ON TABLE gold.sec_filings IS 'SEC filing metadata with categorization';
COMMENT ON TABLE gold.pipeline_audit IS 'Pipeline execution audit and performance metrics';
COMMENT ON TABLE gold.quality_alerts IS 'Data quality monitoring alerts';
