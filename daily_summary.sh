#!/bin/bash

echo "GEX PLATFORM - DAILY SUMMARY"
echo "Date: $(date)"
echo "========================================="
echo ""

psql -U gex_user -d gex_db -h localhost << 'SQL'
-- Today's activity
SELECT 
    'Options Records Collected:' as metric,
    COUNT(*)::TEXT as value
FROM options_quotes
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE

UNION ALL

SELECT 
    'GEX Calculations Performed:',
    COUNT(*)::TEXT
FROM gex_metrics
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE

UNION ALL

SELECT 
    'Unique Strikes Traded:',
    COUNT(DISTINCT strike)::TEXT
FROM options_quotes
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE

UNION ALL

SELECT 
    'Data Collection Start:',
    TO_CHAR(MIN(timestamp) AT TIME ZONE 'America/New_York', 'HH24:MI:SS')
FROM options_quotes
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE

UNION ALL

SELECT 
    'Data Collection End:',
    TO_CHAR(MAX(timestamp) AT TIME ZONE 'America/New_York', 'HH24:MI:SS')
FROM options_quotes
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE;

\echo ''
\echo 'GEX STATISTICS FOR TODAY:'

SELECT 
    TO_CHAR(timestamp AT TIME ZONE 'America/New_York', 'HH24:MI') as time,
    underlying_price as spot,
    ROUND((total_gamma_exposure/1e6)::NUMERIC, 1) as total_gex_M,
    ROUND((net_gex/1e6)::NUMERIC, 1) as net_gex_M,
    max_gamma_strike,
    ROUND(gamma_flip_point::NUMERIC, 2) as flip_point
FROM gex_metrics
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE
ORDER BY timestamp DESC
LIMIT 20;
SQL

echo ""
echo "========================================="
