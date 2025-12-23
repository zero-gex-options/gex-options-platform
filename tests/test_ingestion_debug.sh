#!/bin/bash

echo "========================================"
echo "GEX INGESTION COMPREHENSIVE DEBUG"
echo "========================================"
echo ""

# 1. Check if market is open
echo "1. Market Status:"
cd /home/ubuntu/gex-options-platform
source venv/bin/activate
python scripts/debug_market_hours.py
echo ""

# 2. Check database connection
echo "2. Database Connection Test:"
psql -U gex_user -d gex_db -h localhost -c "SELECT NOW() as current_time;" 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Database connection OK"
else
    echo "❌ Database connection FAILED"
fi
echo ""

# 3. Check if service is running
echo "3. Service Status:"
sudo systemctl is-active gex-ingestion
echo ""

# 4. Check recent logs for key patterns
echo "4. Log Analysis (last 5 minutes):"
echo "   - Stream connections:"
sudo journalctl -u gex-ingestion --since "5 minutes ago" | grep -i "stream" | tail -5
echo ""
echo "   - Options received:"
sudo journalctl -u gex-ingestion --since "5 minutes ago" | grep -i "received" | tail -3
echo ""
echo "   - Batch flushes:"
sudo journalctl -u gex-ingestion --since "5 minutes ago" | grep -i "flush\|storing" | tail -5
echo ""
echo "   - Database operations:"
sudo journalctl -u gex-ingestion --since "5 minutes ago" | grep -i "database\|commit" | tail -5
echo ""
echo "   - Errors:"
sudo journalctl -u gex-ingestion --since "5 minutes ago" | grep -i "error\|failed" | tail -5
echo ""

# 5. Check database records
echo "5. Database Records:"
echo "   Last 5 minutes:"
psql -U gex_user -d gex_db -h localhost -c "SELECT COUNT(*) FROM options_quotes WHERE timestamp > NOW() - INTERVAL '5 minutes';"
echo ""
echo "   Last hour:"
psql -U gex_user -d gex_db -h localhost -c "SELECT COUNT(*) FROM options_quotes WHERE timestamp > NOW() - INTERVAL '1 hour';"
echo ""
echo "   Total records:"
psql -U gex_user -d gex_db -h localhost -c "SELECT COUNT(*) FROM options_quotes;"
echo ""
echo "   Latest timestamp:"
psql -U gex_user -d gex_db -h localhost -c "SELECT MAX(timestamp) FROM options_quotes;"
echo ""

# 6. Check environment variables in service
echo "6. Service Environment Variables:"
sudo systemctl show gex-ingestion | grep -E "DB_HOST|DB_NAME|DB_USER|TRADESTATION_CLIENT_ID" | head -5
echo ""

echo "========================================"
echo "DEBUG COMPLETE"
echo "========================================"
