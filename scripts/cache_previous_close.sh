#!/bin/bash

# ==============================================
# Cache Previous Day's Close from TradeStation
# Runs at 4:00 PM ET daily (or manually)
#
# Usage:
#   ./cache_previous_close.sh              # Cache today's close (default)
#   ./cache_previous_close.sh 2026-02-05   # Cache specific date's close
# ==============================================

sleep 1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Get target date from command line or use today
TARGET_DATE="${1:-today}"

cd "$PROJECT_ROOT"
source venv/bin/activate

python3 << PYTHON
import os
import sys
import json
import requests
from pathlib import Path
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path.cwd()))

from src.ingestion.tradestation_client import TradeStationSimpleClient

target_date = "${TARGET_DATE}"
eastern = pytz.timezone('America/New_York')
now_et = datetime.now(eastern)

print(f"[{now_et}] Caching previous close from TradeStation...")
print(f"Target date: {target_date}")

try:
    # Parse target date
    if target_date == "today":
        cache_for_date = now_et.date()
        # Get today's close for use tomorrow
        bars_back = 1
        print(f"Using today's close as tomorrow's previous close")
    else:
        # Parse the date string (YYYY-MM-DD)
        cache_for_date = datetime.strptime(target_date, '%Y-%m-%d').date()

        # Calculate how many bars back we need to go
        days_diff = (now_et.date() - cache_for_date).days

        if days_diff < 0:
            print(f"ERROR: Cannot cache future date {target_date}")
            sys.exit(1)

        # Add extra days for weekends/holidays
        bars_back = days_diff + 5  # Get more bars to ensure we have the date we want
        print(f"Fetching {bars_back} bars back to find {target_date}")

    # Create TradeStation client
    ts_client = TradeStationSimpleClient(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    )

    # Get daily bars
    headers = ts_client.auth.get_headers()
    headers['Content-Type'] = 'application/json'

    url = f"{ts_client.base_url}/marketdata/barcharts/SPY"
    params = {
        'unit': 'Daily',
        'barsback': str(bars_back),
        'sessiontemplate': 'USEQ24Hour'
    }

    response = requests.get(url, headers=headers, params=params, timeout=10)

    if response.status_code != 200:
        print(f"ERROR: TradeStation API returned status {response.status_code}")
        print(response.text)
        sys.exit(1)

    data = response.json()

    if 'Bars' not in data or len(data['Bars']) == 0:
        print("ERROR: No bar data from TradeStation")
        sys.exit(1)

    # Find the bar for the target date
    target_bar = None

    if target_date == "today":
        # Use the most recent bar
        target_bar = data['Bars'][-1]
        trading_date_str = target_bar.get('TimeStamp', '')
        # For today, cache for tomorrow
        cache_date = (cache_for_date + timedelta(days=1)).isoformat()
    else:
        # Search for the specific date
        target_date_str = cache_for_date.isoformat()

        for bar in reversed(data['Bars']):
            bar_timestamp = bar.get('TimeStamp', '')
            # Parse the timestamp (format: "2026-02-05T00:00:00Z")
            if bar_timestamp.startswith(target_date_str):
                target_bar = bar
                break

        if target_bar is None:
            print(f"ERROR: Could not find bar data for {target_date_str}")
            print(f"Available dates:")
            for bar in data['Bars'][-10:]:
                print(f"  - {bar.get('TimeStamp', 'N/A')}")
            sys.exit(1)

        trading_date_str = target_bar.get('TimeStamp', '')
        # Cache for the date itself (this IS the previous close for that date)
        cache_date = cache_for_date.isoformat()

    close_price = float(target_bar['Close'])

    # Create cache data
    cache_data = {
        'date': cache_date,
        'prev_close': close_price,
        'cached_at': now_et.isoformat(),
        'trading_date': trading_date_str
    }

    # Write to cache file
    cache_file = Path("/data/monitoring/spy_previous_close.json")
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    with open(cache_file, 'w') as f:
        json.dump(cache_data, f, indent=2)

    print(f"✅ Cached previous close: \${close_price:.2f}")
    print(f"   For date: {cache_date}")
    print(f"   Trading date: {trading_date_str}")
    print(f"   Cache file: {cache_file}")

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

PYTHON
