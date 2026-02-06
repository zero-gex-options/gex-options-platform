#!/bin/bash

# ==============================================
# Cache Previous Day's Close from TradeStation
# Runs at 4:00 PM ET daily (or manually)
#
# Usage:
#   ./cache_previous_close.sh              # Cache based on current time
#   ./cache_previous_close.sh 2026-02-05   # Cache specific date's close
# ==============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"
source venv/bin/activate

# Determine target date based on current time or argument
if [ -z "$1" ]; then
    # No argument - use current time logic
    CURRENT_HOUR=$(TZ='America/New_York' date +%H)
    
    if [ "$CURRENT_HOUR" -lt 16 ]; then
        # Before 4:00 PM ET - use yesterday's date
        TARGET_DATE=$(TZ='America/New_York' date -d "yesterday" +%Y-%m-%d)
        CACHE_FOR_DATE=$(TZ='America/New_York' date +%Y-%m-%d)
        echo "Before 4:00 PM ET - fetching previous day's close ($TARGET_DATE) for use today ($CACHE_FOR_DATE)"
    else
        # At/after 4:00 PM ET - use today's date
        TARGET_DATE=$(TZ='America/New_York' date +%Y-%m-%d)
        CACHE_FOR_DATE=$(TZ='America/New_York' date -d "tomorrow" +%Y-%m-%d)
        echo "At/after 4:00 PM ET - fetching today's close ($TARGET_DATE) for use tomorrow ($CACHE_FOR_DATE)"
    fi
else
    # Manual date provided
    TARGET_DATE="$1"
    CACHE_FOR_DATE="$TARGET_DATE"
    echo "Manual mode - fetching close for $TARGET_DATE"
fi

echo "Calling TradeStation API for $TARGET_DATE..."

# Call tradestation_client.py to get the quote for the target date
# Using --unit Daily --bars-back 1 --last-date to get the close for that specific day
QUOTE_OUTPUT=$(python -m src.ingestion.tradestation_client \
    --quote \
    --symbol SPY \
    --unit Daily \
    --bars-back 1 \
    --last-date "${TARGET_DATE}T23:59:59Z" 2>&1)

# Extract the close price from the output
# Looking for line like "   Close: $612.44" or "   close: 612.44"
CLOSE_PRICE=$(echo "$QUOTE_OUTPUT" | grep -i "close:" | grep -oE '[0-9]+\.[0-9]+' | head -1)

if [ -z "$CLOSE_PRICE" ]; then
    echo "ERROR: Failed to extract close price from TradeStation"
    echo "Output was:"
    echo "$QUOTE_OUTPUT"
    exit 1
fi

echo "Retrieved close price: \$$CLOSE_PRICE for $TARGET_DATE"

# Create cache directory if needed
mkdir -p /data/monitoring

# Write cache file
CACHED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

cat > /data/monitoring/spy_previous_close.json << EOF
{
  "date": "$CACHE_FOR_DATE",
  "prev_close": $CLOSE_PRICE,
  "trading_date": "$TARGET_DATE",
  "cached_at": "$CACHED_AT"
}
EOF

echo "✅ Successfully cached SPY previous close"
echo "   Close Price: \$$CLOSE_PRICE"
echo "   Trading Date: $TARGET_DATE"
echo "   Cache Valid For: $CACHE_FOR_DATE"
echo "   Cache File: /data/monitoring/spy_previous_close.json"

# Display the cache file
echo ""
echo "Cache contents:"
cat /data/monitoring/spy_previous_close.json
