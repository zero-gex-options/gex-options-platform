-- GEX Options Platform Database Schema
-- PostgreSQL with TimescaleDB extension

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop existing tables if recreating
DROP TABLE IF EXISTS gex_metrics CASCADE;
DROP TABLE IF EXISTS options_quotes CASCADE;
DROP TABLE IF EXISTS underlying_quotes CASCADE;
DROP TABLE IF EXISTS ingestion_metrics CASCADE;

-- Underlying quotes table
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
    source TEXT,
    PRIMARY KEY (timestamp, symbol)
);

-- Convert to hypertable
SELECT create_hypertable('underlying_quotes', 'timestamp');

-- Options quotes table
CREATE TABLE options_quotes (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    underlying_price DOUBLE PRECISION,
    strike DOUBLE PRECISION NOT NULL,
    expiration DATE NOT NULL,
    dte INTEGER,
    option_type TEXT NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    mid DOUBLE PRECISION,
    last DOUBLE PRECISION,
    volume BIGINT,
    open_interest BIGINT,
    implied_vol DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    rho DOUBLE PRECISION,
    is_calculated BOOLEAN DEFAULT FALSE,
    spread_pct DOUBLE PRECISION,
    source TEXT,
    PRIMARY KEY (timestamp, symbol, strike, expiration, option_type)
);

-- Convert to hypertable
SELECT create_hypertable('options_quotes', 'timestamp');

-- GEX metrics table
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
    PRIMARY KEY (timestamp, symbol, expiration)
);

-- Convert to hypertable
SELECT create_hypertable('gex_metrics', 'timestamp');

-- Ingestion metrics table
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

-- Service uptime tracking table
CREATE TABLE service_uptime_checks (
    timestamp TIMESTAMPTZ NOT NULL,
    service_name TEXT NOT NULL,
    is_up INTEGER NOT NULL,  -- 1 for up, 0 for down
    PRIMARY KEY (timestamp, service_name)
);

-- Convert to hypertable
SELECT create_hypertable('service_uptime_checks', 'timestamp');

-- Indexes for performance
CREATE INDEX idx_options_quotes_symbol_exp ON options_quotes(symbol, expiration, timestamp DESC);
CREATE INDEX idx_options_quotes_strike ON options_quotes(strike, timestamp DESC);
CREATE INDEX idx_gex_metrics_symbol ON gex_metrics(symbol, timestamp DESC);
CREATE INDEX idx_underlying_quotes_symbol ON underlying_quotes(symbol, timestamp DESC);
CREATE INDEX idx_service_uptime_service ON service_uptime_checks(service_name, timestamp DESC);

-- Compression policy (compress data older than 7 days)
SELECT add_compression_policy('options_quotes', INTERVAL '7 days');
SELECT add_compression_policy('underlying_quotes', INTERVAL '7 days');
SELECT add_compression_policy('gex_metrics', INTERVAL '7 days');
SELECT add_compression_policy('ingestion_metrics', INTERVAL '30 days');
SELECT add_compression_policy('service_uptime_checks', INTERVAL '7 days');

-- Retention policy (keep data for 90 days)
SELECT add_retention_policy('options_quotes', INTERVAL '90 days');
SELECT add_retention_policy('underlying_quotes', INTERVAL '90 days');
SELECT add_retention_policy('gex_metrics', INTERVAL '90 days');
SELECT add_retention_policy('ingestion_metrics', INTERVAL '90 days');
SELECT add_retention_policy('service_uptime_checks', INTERVAL '90 days');

-- Views for common queries
CREATE OR REPLACE VIEW latest_gex AS
SELECT DISTINCT ON (symbol, expiration)
    *
FROM gex_metrics
ORDER BY symbol, expiration, timestamp DESC;

CREATE OR REPLACE VIEW latest_underlying_quotes AS
SELECT DISTINCT ON (symbol)
    *
FROM underlying_quotes
ORDER BY symbol, timestamp DESC;

-- Grant permissions (adjust as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO gex_user;

-- Success message
DO $
BEGIN
    RAISE NOTICE 'Database schema created successfully!';
    RAISE NOTICE 'Tables: underlying_quotes, options_quotes, gex_metrics, ingestion_metrics';
    RAISE NOTICE 'Views: latest_gex, latest_underlying_quotes';
END $;
