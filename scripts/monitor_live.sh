#!/bin/bash

while true; do
    clear
    echo "=== LIVE DATA MONITOR (updates every 5 sec) ==="
    echo "Press Ctrl+C to stop"
    echo ""
    date
    echo ""
    
    psql -U gex_user -d gex_db -h localhost << 'SQL'
-- Last 5 minutes
SELECT 
    'Last 5 min:' as period,
    COUNT(*) as records,
    MAX(timestamp) AT TIME ZONE 'America/New_York' as latest_et
FROM options_quotes
WHERE timestamp > NOW() - INTERVAL '5 minutes';

-- Last record details
SELECT 
    'Latest option:' as info,
    symbol || ' ' || strike || option_type as option_details,
    timestamp AT TIME ZONE 'America/New_York' as et_time
FROM options_quotes
ORDER BY timestamp DESC
LIMIT 1;
SQL
    
    sleep 5
done
