#!/bin/bash

echo "========================================="
echo "GEX PLATFORM HEALTH CHECK"
echo "========================================="
date
echo ""

echo "SERVICES:"
echo "  Ingestion: $(systemctl is-active gex-ingestion)"
echo "  Scheduler: $(systemctl is-active gex-scheduler)"
echo "  PostgreSQL: $(systemctl is-active postgresql)"
echo ""

echo "DATA FRESHNESS:"
psql -U gex_user -d gex_db -h localhost -t << 'SQL'
SELECT 
    'Options data: ' || 
    EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))::INT || 's ago (' ||
    COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 minute') || ' records last min)'
FROM options_quotes
UNION ALL
SELECT 
    'GEX metrics: ' || 
    EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))::INT || 's ago'
FROM gex_metrics;
SQL

echo ""
echo "TODAY'S STATS:"
psql -U gex_user -d gex_db -h localhost -t << 'SQL'
SELECT 
    'Options records: ' || COUNT(*)::TEXT
FROM options_quotes
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE
UNION ALL
SELECT 
    'GEX calculations: ' || COUNT(*)::TEXT
FROM gex_metrics
WHERE DATE(timestamp AT TIME ZONE 'America/New_York') = CURRENT_DATE;
SQL

echo ""
echo "LATEST GEX:"
psql -U gex_user -d gex_db -h localhost -t << 'SQL'
SELECT 
    '  Time: ' || (timestamp AT TIME ZONE 'America/New_York')::TEXT ||
    ', Spot: $' || ROUND(underlying_price::NUMERIC, 2)::TEXT ||
    ', Total GEX: $' || ROUND((total_gamma_exposure/1e6)::NUMERIC, 1)::TEXT || 'M' ||
    ', Net GEX: $' || ROUND((net_gex/1e6)::NUMERIC, 1)::TEXT || 'M'
FROM gex_metrics
ORDER BY timestamp DESC
LIMIT 1;
SQL

echo ""
echo "========================================="
