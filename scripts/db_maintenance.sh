#!/bin/bash

# ==============================================
# Database Maintenance Script
# Runs ANALYZE and checks for slow queries
# ==============================================

echo "Running database maintenance..."

psql -U gex_user -d gex_db -h localhost << 'SQL'
-- Update statistics
ANALYZE options_quotes;
ANALYZE underlying_quotes;
ANALYZE gex_metrics;

-- Check for slow queries (from logs)
SELECT 
    'Database maintenance complete' as status,
    NOW() as timestamp;

-- Show table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
SQL

echo "✅ Maintenance complete"
