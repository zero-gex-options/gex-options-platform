-- GEX Options Platform Database Schema
-- PostgreSQL with TimescaleDB extension

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop existing tables if recreating
DROP TABLE IF EXISTS gex_metrics CASCADE;
DROP TABLE IF EXISTS options_quotes CASCADE;
DROP TABLE IF EXISTS underlying_quotes CASCADE;
DROP TABLE IF EXISTS ingestion_metrics CASCADE;
DROP TABLE IF EXISTS service_uptime_checks CASCADE;

-- ============================================================================
-- Underlying quotes table (TIME-SERIES)
-- ============================================================================
CREATE TABLE underlying_quotes (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    total_volume BIGINT,
    up_volume BIGINT,
    down_volume BIGINT,
    actual_time TIMESTAMPTZ,  -- Actual time when quote was received
    PRIMARY KEY (timestamp, symbol)
);

-- Convert to hypertable
SELECT create_hypertable('underlying_quotes', 'timestamp');

-- ============================================================================
-- Options quotes table (LATEST STATE - NOT TIME-SERIES)
-- ============================================================================
-- This table stores the LATEST quote for each option contract.
-- Each contract (symbol/strike/expiration/type) has exactly one row.
-- Historical tick data is NOT stored.
CREATE TABLE options_quotes (
    -- Primary key fields (no timestamp!)
    symbol TEXT NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    expiration DATE NOT NULL,
    option_type TEXT NOT NULL,

    -- Market data
    underlying_price DOUBLE PRECISION,
    dte INTEGER,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    mid DOUBLE PRECISION,
    last DOUBLE PRECISION,
    volume BIGINT,
    open_interest BIGINT,
    implied_vol DOUBLE PRECISION,

    -- Greeks
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    rho DOUBLE PRECISION,
    is_calculated BOOLEAN DEFAULT FALSE,

    -- Metadata
    spread_pct DOUBLE PRECISION,
    source TEXT,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Primary key: one row per contract
    PRIMARY KEY (symbol, strike, expiration, option_type)
);

-- NOTE: options_quotes is NOT a hypertable because it stores latest state, not time-series

-- ============================================================================
-- GEX metrics table (TIME-SERIES)
-- ============================================================================
CREATE TABLE gex_metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    expiration DATE NOT NULL,
    underlying_price DOUBLE PRECISION,
    total_gamma_exposure DOUBLE PRECISION,
    call_gamma DOUBLE PRECISION,
    put_gamma DOUBLE PRECISION,
    net_gex DOUBLE PRECISION,
    call_volume BIGINT,
    put_volume BIGINT,
    call_oi BIGINT,
    put_oi BIGINT,
    total_contracts BIGINT,
    max_gamma_strike DOUBLE PRECISION,
    max_gamma_value DOUBLE PRECISION,
    gamma_flip_point DOUBLE PRECISION,
    put_call_ratio DOUBLE PRECISION,
    vanna_exposure DOUBLE PRECISION,
    charm_exposure DOUBLE PRECISION,
    max_pain DOUBLE PRECISION,
    PRIMARY KEY (timestamp, symbol, expiration)
);

-- Convert to hypertable
SELECT create_hypertable('gex_metrics', 'timestamp');

-- ============================================================================
-- Ingestion metrics table (TIME-SERIES)
-- ============================================================================
CREATE TABLE ingestion_metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    records_ingested BIGINT,
    records_stored BIGINT,
    error_count BIGINT,
    heartbeat_count BIGINT,
    last_heartbeat TIMESTAMPTZ,
    processing_time_ms BIGINT,
    PRIMARY KEY (timestamp, source, symbol)
);

-- Convert to hypertable
SELECT create_hypertable('ingestion_metrics', 'timestamp');

-- ============================================================================
-- Service uptime tracking table (TIME-SERIES)
-- ============================================================================
CREATE TABLE service_uptime_checks (
    timestamp TIMESTAMPTZ NOT NULL,
    service_name TEXT NOT NULL,
    is_up INTEGER NOT NULL,  -- 1 for up, 0 for down
    PRIMARY KEY (timestamp, service_name)
);

-- Convert to hypertable
SELECT create_hypertable('service_uptime_checks', 'timestamp');

-- ============================================================================
-- Indexes for options_quotes (latest state table)
-- ============================================================================

-- Index for queries by underlying symbol (e.g., "SPY")
CREATE INDEX idx_options_quotes_underlying 
ON options_quotes ((substring(symbol from 1 for 3)), expiration, last_updated DESC);

-- Index for GEX calculations (gamma + open interest)
CREATE INDEX idx_options_quotes_gex 
ON options_quotes ((substring(symbol from 1 for 3)), expiration, gamma, open_interest)
WHERE gamma IS NOT NULL AND gamma > 0;

-- Index for strike lookups
CREATE INDEX idx_options_quotes_strike 
ON options_quotes (strike, expiration);

-- Index for expiration date queries
CREATE INDEX idx_options_quotes_expiration 
ON options_quotes (expiration, last_updated DESC);

-- ============================================================================
-- Indexes for time-series tables
-- ============================================================================

-- Underlying quotes indexes
CREATE INDEX idx_underlying_quotes_symbol 
ON underlying_quotes(symbol, timestamp DESC);

CREATE INDEX idx_underlying_quotes_spy_timestamp_agg
ON underlying_quotes(symbol, timestamp DESC)
WHERE symbol = 'SPY';

-- GEX metrics indexes
CREATE INDEX idx_gex_metrics_symbol 
ON gex_metrics(symbol, timestamp DESC);

-- Service uptime indexes
CREATE INDEX idx_service_uptime_service 
ON service_uptime_checks(service_name, timestamp DESC);

-- ============================================================================
-- Compression policies (for hypertables only)
-- ============================================================================
SELECT add_compression_policy('underlying_quotes', INTERVAL '1 day');
SELECT add_compression_policy('gex_metrics', INTERVAL '7 days');
SELECT add_compression_policy('ingestion_metrics', INTERVAL '7 days');
SELECT add_compression_policy('service_uptime_checks', INTERVAL '7 days');

-- ============================================================================
-- Retention policies (for hypertables only)
-- ============================================================================
SELECT add_retention_policy('underlying_quotes', INTERVAL '2 days');
SELECT add_retention_policy('gex_metrics', INTERVAL '30 days');
SELECT add_retention_policy('ingestion_metrics', INTERVAL '30 days');
SELECT add_retention_policy('service_uptime_checks', INTERVAL '30 days');

-- NOTE: No retention policy on options_quotes - it only stores latest state

-- ============================================================================
-- Views for common queries
-- ============================================================================

-- Latest GEX metrics
CREATE OR REPLACE VIEW latest_gex AS
SELECT DISTINCT ON (symbol, expiration)
    *
FROM gex_metrics
ORDER BY symbol, expiration, timestamp DESC;

-- Latest underlying quotes
CREATE OR REPLACE VIEW latest_underlying_quotes AS
SELECT DISTINCT ON (symbol)
    *
FROM underlying_quotes
ORDER BY symbol, timestamp DESC;

-- Latest options for GEX calculations
CREATE OR REPLACE VIEW latest_options_for_gex AS
SELECT 
    symbol,
    strike,
    option_type,
    gamma,
    delta,
    vega,
    open_interest,
    volume,
    underlying_price,
    expiration,
    last_updated as timestamp
FROM options_quotes
WHERE gamma IS NOT NULL
    AND gamma > 0
    AND last_updated > NOW() - INTERVAL '1 hour';

-- ============================================================================
-- Permissions
-- ============================================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO gex_user;

-- ============================================================================
-- Comments and documentation
-- ============================================================================

COMMENT ON TABLE options_quotes IS 
'Latest state of all option contracts. Each contract (symbol/strike/expiration/type) has exactly one row that is updated with each new quote. Historical tick data is not stored.';

COMMENT ON COLUMN options_quotes.last_updated IS 
'Timestamp when this quote was last updated. Used to filter stale data.';

COMMENT ON TABLE underlying_quotes IS 
'Time-series of underlying price quotes. Stores historical data.';

COMMENT ON TABLE gex_metrics IS 
'Time-series of gamma exposure calculations. Stores historical GEX data.';

COMMENT ON VIEW latest_options_for_gex IS 
'Active options contracts for GEX calculations. Filters to data updated within the last hour with valid gamma values.';

-- Success message
DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE '✅ Database schema created successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - underlying_quotes (hypertable - time-series)';
    RAISE NOTICE '  - options_quotes (regular table - latest state only)';
    RAISE NOTICE '  - gex_metrics (hypertable - time-series)';
    RAISE NOTICE '  - ingestion_metrics (hypertable - time-series)';
    RAISE NOTICE '  - service_uptime_checks (hypertable - time-series)';
    RAISE NOTICE '';
    RAISE NOTICE 'Views created:';
    RAISE NOTICE '  - latest_gex';
    RAISE NOTICE '  - latest_underlying_quotes';
    RAISE NOTICE '  - latest_options_for_gex';
    RAISE NOTICE '';
    RAISE NOTICE 'Key design decision:';
    RAISE NOTICE '  - options_quotes stores LATEST STATE only (one row per contract)';
    RAISE NOTICE '  - Other tables store TIME-SERIES data (multiple rows over time)';
    RAISE NOTICE '============================================================================';
END $$;
