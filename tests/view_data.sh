#!/bin/bash

echo "=== GEX Database Viewer ==="
echo ""

psql -U gex_user -d gex_db -h localhost << 'SQL'
-- Summary
\echo '1. DATABASE SUMMARY'
\echo '==================='
SELECT 
    COUNT(*) as total_records,
    MIN(timestamp) as first_record,
    MAX(timestamp) as last_record
FROM options_quotes;

\echo ''
\echo '2. RECORDS BY DATE'
\echo '=================='
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as records,
    COUNT(DISTINCT strike) as unique_strikes
FROM options_quotes
GROUP BY DATE(timestamp)
ORDER BY date DESC
LIMIT 7;

\echo ''
\echo '3. MOST RECENT 5 OPTIONS'
\echo '========================'
SELECT 
    timestamp AT TIME ZONE 'America/New_York' as et_time,
    symbol || ' ' || strike || ' ' || UPPER(option_type) as option,
    bid,
    ask,
    volume,
    open_interest
FROM options_quotes
ORDER BY timestamp DESC
LIMIT 5;

\echo ''
\echo '4. RECORDS BY HOUR (TODAY)'
\echo '=========================='
SELECT 
    TO_CHAR(timestamp, 'HH24:00') as hour,
    COUNT(*) as records
FROM options_quotes
WHERE DATE(timestamp) = CURRENT_DATE
GROUP BY TO_CHAR(timestamp, 'HH24:00')
ORDER BY hour DESC;

\echo ''
\echo '5. DATA STATISTICS'
\echo '=================='
SELECT 
    COUNT(DISTINCT symbol) as symbols,
    COUNT(DISTINCT strike) as unique_strikes,
    AVG(volume)::INT as avg_volume,
    AVG(open_interest)::INT as avg_oi
FROM options_quotes
WHERE timestamp > NOW() - INTERVAL '1 hour';

SQL
