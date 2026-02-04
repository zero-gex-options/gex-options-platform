#!/usr/bin/env python3
"""
ZeroGEX Monitoring Web Dashboard
Simple Flask app to display monitoring metrics
"""

from flask import Flask, jsonify, send_from_directory
import json
from pathlib import Path
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import traceback
from datetime import datetime
import pytz

app = Flask(__name__)
METRICS_FILE = Path("/home/ubuntu/monitoring/current_metrics.json")
DASHBOARD_DIR = Path("/opt/zerogex/monitoring")
CREDS_FILE = Path.home() / ".zerogex_db_creds"

# Global connection pool
db_pool = None

def load_db_config():
    """Load database configuration from ~/.zerogex_db_creds"""
    try:
        if not CREDS_FILE.exists():
            print(f"Credentials file not found: {CREDS_FILE}")
            return None

        config = {}
        with open(CREDS_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key] = value

        return {
            'host': config.get('DB_HOST', 'localhost'),
            'port': int(config.get('DB_PORT', '5432')),
            'database': config.get('DB_NAME', 'gex_db'),
            'user': config.get('DB_USER', 'gex_user'),
            'password': config.get('DB_PASSWORD', ''),
        }
    except Exception as e:
        print(f"Error loading DB config: {e}")
        traceback.print_exc()
        return None

def init_db_pool():
    """Initialize database connection pool"""
    global db_pool
    if db_pool is None:
        db_config = load_db_config()
        if db_config:
            try:
                db_pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=5,
                    connect_timeout=3,
                    **db_config
                )
                print("Database connection pool initialized")
            except Exception as e:
                print(f"Error creating connection pool: {e}")
                db_pool = None

def get_db_connection():
    """Get a connection from the pool"""
    global db_pool
    if db_pool is None:
        init_db_pool()

    if db_pool:
        try:
            return db_pool.getconn()
        except Exception as e:
            print(f"Error getting connection from pool: {e}")
            return None
    return None

def return_db_connection(conn):
    """Return a connection to the pool"""
    global db_pool
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            print(f"Error returning connection to pool: {e}")

@app.route('/')
def dashboard():
    try:
        return send_from_directory(DASHBOARD_DIR, 'dashboard.html')
    except Exception as e:
        print(f"Error serving dashboard: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics')
def get_metrics():
    import time
    max_retries = 3

    for attempt in range(max_retries):
        try:
            if METRICS_FILE.exists():
                # Use with statement and explicit error handling
                with open(METRICS_FILE, 'r') as f:
                    content = f.read()

                # Validate it's not empty
                if not content or len(content) < 10:
                    print(f"Metrics file is empty or too small (attempt {attempt + 1})")
                    time.sleep(0.1)
                    continue

                try:
                    data = json.loads(content)
                    # Ensure timestamp is valid
                    if not data.get('timestamp'):
                        data['timestamp'] = datetime.now(pytz.utc).isoformat()
                    return jsonify(data)
                except json.JSONDecodeError as e:
                    print(f"JSON decode error (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(0.1)
                        continue
                    else:
                        # Return a minimal valid response
                        return jsonify({
                            'error': 'Metrics temporarily unavailable',
                            'timestamp': datetime.now(pytz.utc).isoformat(),
                            'market_open': False,
                            'system': {
                                'cpu_percent': 0,
                                'memory_percent': 0,
                                'disk_percent': 0,
                                'memory_used_gb': 0,
                                'memory_total_gb': 0,
                                'disk_used_gb': 0,
                                'disk_total_gb': 0
                            },
                            'services': {},
                            'database': {'error': 'Unavailable'},
                            'alerts': []
                        })
            else:
                return jsonify({
                    'error': 'Metrics file not found',
                    'timestamp': datetime.now(pytz.utc).isoformat(),
                    'market_open': False,
                    'system': {
                        'cpu_percent': 0,
                        'memory_percent': 0,
                        'disk_percent': 0,
                        'memory_used_gb': 0,
                        'memory_total_gb': 0,
                        'disk_used_gb': 0,
                        'disk_total_gb': 0
                    },
                    'services': {},
                    'database': {'error': 'Unavailable'},
                    'alerts': []
                }), 404

        except Exception as e:
            print(f"Error reading metrics file (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(0.1)
                continue
            else:
                traceback.print_exc()
                return jsonify({
                    'error': str(e),
                    'timestamp': datetime.now(pytz.utc).isoformat(),
                    'market_open': False,
                    'system': {
                        'cpu_percent': 0,
                        'memory_percent': 0,
                        'disk_percent': 0,
                        'memory_used_gb': 0,
                        'memory_total_gb': 0,
                        'disk_used_gb': 0,
                        'disk_total_gb': 0
                    },
                    'services': {},
                    'database': {'error': 'Unavailable'},
                    'alerts': []
                }), 500

    # Shouldn't reach here, but just in case
    return jsonify({'error': 'Failed after retries'}), 500

@app.route('/api/table/<table_name>')
def get_table_data(table_name):
    """Get recent 100 rows from a database table"""
    allowed_tables = {
        'options_quotes': 'options_quotes',
        'underlying_quotes': 'underlying_quotes',
        'gex_metrics': 'gex_metrics',
        'ingestion_metrics': 'ingestion_metrics'
    }

    if table_name not in allowed_tables:
        return jsonify({'error': 'Invalid table name'}), 400

    actual_table = allowed_tables[table_name]
    conn = None

    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(f"""
            SELECT * FROM {actual_table}
            ORDER BY timestamp DESC
            LIMIT 100
        """)

        rows = cursor.fetchall()
        cursor.close()

        return jsonify([dict(row) for row in rows])
    except Exception as e:
        print(f"Error getting table data for {table_name}: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)


@app.route('/api/spy-history')
def get_spy_history():
    """Get SPY price history with open interest data for charts"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")
        cursor.execute("SET statement_timeout = '5s'")

        # SPY query
        print("Fetching SPY aggregated data (5-min buckets)...")
        cursor.execute("""
            WITH five_min_buckets AS (
                SELECT
                    date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') +
                    (floor(EXTRACT(minute FROM timestamp AT TIME ZONE 'America/New_York') / 5) * INTERVAL '5 minutes') as bucket_time,
                    (array_agg(open ORDER BY timestamp))[1] as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (array_agg(close ORDER BY timestamp DESC))[1] as close,
                    MAX(timestamp) as actual_timestamp,
                    SUM(COALESCE(total_volume, 0)) as volume,
                    SUM(COALESCE(up_volume, 0)) as up_volume,
                    SUM(COALESCE(down_volume, 0)) as down_volume
                FROM underlying_quotes
                WHERE symbol = 'SPY'
                  AND timestamp > NOW() - INTERVAL '48 hours'
                GROUP BY bucket_time
            )
            SELECT * FROM five_min_buckets
            ORDER BY bucket_time ASC
            LIMIT 600
        """)
        price_data = cursor.fetchall()

        # OI query with total and notional values
        cursor.execute("""
            SELECT
                date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') +
                (floor(EXTRACT(minute FROM timestamp AT TIME ZONE 'America/New_York') / 5) * INTERVAL '5 minutes') as bucket_time,
                -- Contract counts
                SUM(CASE WHEN option_type = 'call' THEN COALESCE(open_interest, 0) ELSE 0 END) as call_oi,
                SUM(CASE WHEN option_type = 'put' THEN COALESCE(open_interest, 0) ELSE 0 END) as put_oi,
                -- Notional values (OI * mid price * 100 shares per contract)
                SUM(CASE WHEN option_type = 'call' THEN COALESCE(open_interest, 0) * COALESCE(mid, 0) * 100 ELSE 0 END) as call_notional,
                SUM(CASE WHEN option_type = 'put' THEN COALESCE(open_interest, 0) * COALESCE(mid, 0) * 100 ELSE 0 END) as put_notional,
                COUNT(*) as quote_count
            FROM options_quotes
            WHERE symbol LIKE 'SPY%'
              AND timestamp > NOW() - INTERVAL '48 hours'
              AND mid > 0  -- Only include options with valid mid price
            GROUP BY bucket_time
            ORDER BY bucket_time ASC
            LIMIT 600
        """)
        oi_data = cursor.fetchall()

        print(f"OI query returned {len(oi_data)} buckets")
        if len(oi_data) > 0:
            sample = oi_data[-1]  # Most recent
            print(f"Most recent OI bucket: time={sample['bucket_time']}, "
                  f"call_oi={sample['call_oi']}, put_oi={sample['put_oi']}, "
                  f"call_notional=${sample['call_notional']/1e6:.1f}M, put_notional=${sample['put_notional']/1e6:.1f}M")

        cursor.close()

        # Create OI map - use string keys for better matching
        oi_map = {}
        for row in oi_data:
            ts_key = row['bucket_time']
            oi_map[ts_key] = {
                'call_oi': int(row['call_oi'] or 0),
                'put_oi': int(row['put_oi'] or 0),
                'call_notional': float(row['call_notional'] or 0),
                'put_notional': float(row['put_notional'] or 0),
                'quote_count': int(row['quote_count'] or 0)
            }

        print(f"Created OI map with {len(oi_map)} entries")
        if len(oi_map) > 0:
            print(f"Sample OI map entry: {list(oi_map.values())[0]}")

        eastern = pytz.timezone('America/New_York')

        # Combine data
        result = []
        for row in price_data:
            ts = row['bucket_time']
            if not ts:
                continue

            # Match OI using the original bucket_time (before timezone conversion)
            oi_info = oi_map.get(ts, {
                'call_oi': 0,
                'put_oi': 0,
                'call_notional': 0,
                'put_notional': 0,
                'quote_count': 0
            })

            # Now convert to ET for output
            if ts.tzinfo is None:
                ts_display = eastern.localize(ts)
            else:
                ts_display = ts.astimezone(eastern)

            # Convert the actual timestamp to ET
            actual_ts = row['actual_timestamp']
            if actual_ts:
                if actual_ts.tzinfo is None:
                    actual_ts_display = eastern.localize(actual_ts)
                else:
                    actual_ts_display = actual_ts.astimezone(eastern)
            else:
                actual_ts_display = ts_display

            result.append({
                'timestamp': ts_display.isoformat(),
                'actual_timestamp': actual_ts_display.isoformat(),
                'open': float(row['open'] or row['close'] or 0),
                'high': float(row['high'] or row['close'] or 0),
                'low': float(row['low'] or row['close'] or 0),
                'close': float(row['close'] or 0),
                'volume': int(row['volume'] or 0),
                'up_volume': int(row['up_volume'] or 0),
                'down_volume': int(row['down_volume'] or 0),
                'call_oi': oi_info['call_oi'],
                'put_oi': oi_info['put_oi'],
                'call_notional': oi_info['call_notional'],
                'put_notional': oi_info['put_notional']
            })

        print(f"Returning {len(result)} combined data points")
        if len(result) > 0:
            print(f"Sample result with notional: call_notional=${result[0]['call_notional']/1e6:.1f}M, "
                  f"put_notional=${result[0]['put_notional']/1e6:.1f}M")

        return jsonify(result)

    except psycopg2.extensions.QueryCanceledError:
        print("SPY history query timeout")
        return jsonify({'error': 'Query timeout'}), 504
    except Exception as e:
        print(f"Error in spy-history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/ingestion-history')
def get_ingestion_history():
    """Get ingestion metrics history for charts"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")
        cursor.execute("SET statement_timeout = '3s'")

        cursor.execute("""
            SELECT 
                date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') as hour,
                SUM(COALESCE(records_ingested, 0)) as records_ingested,
                SUM(COALESCE(error_count, 0)) as error_count
            FROM ingestion_metrics
            WHERE timestamp > NOW() - INTERVAL '48 hours'
            GROUP BY hour
            ORDER BY hour ASC
            LIMIT 100
        """)

        rows = cursor.fetchall()
        cursor.close()

        eastern = pytz.timezone('America/New_York')
        result = []
        for row in rows:
            ts = row['hour']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            result.append({
                'timestamp': ts.isoformat(),
                'records_ingested': int(row['records_ingested']),
                'error_count': int(row['error_count'])
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in ingestion-history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/uptime-history')
def get_uptime_history():
    """Get service uptime history based on actual service checks"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")
        cursor.execute("SET statement_timeout = '3s'")

        cursor.execute("""
            WITH hourly_checks AS (
                SELECT
                    date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') as hour,
                    AVG(CASE WHEN is_up = 1 THEN 100.0 ELSE 0.0 END) as uptime_percent
                FROM service_uptime_checks
                WHERE service_name = 'gex-ingestion'
                  AND timestamp > NOW() - INTERVAL '48 hours'
                GROUP BY date_trunc('hour', timestamp AT TIME ZONE 'America/New_York')
            )
            SELECT
                hour,
                ROUND(uptime_percent::numeric, 1) as uptime_percent
            FROM hourly_checks
            ORDER BY hour ASC
            LIMIT 100
        """)

        rows = cursor.fetchall()
        cursor.close()

        eastern = pytz.timezone('America/New_York')
        result = []
        for row in rows:
            ts = row['hour']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            result.append({
                'timestamp': int(ts.timestamp() * 1000),
                'uptime_percent': float(row['uptime_percent'] or 0)
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in uptime-history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/spy-change')
def get_spy_change():
    """Get SPY price change from previous close"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get current price and previous day's close
        cursor.execute("""
            WITH latest_price AS (
                SELECT close as current_price, timestamp
                FROM underlying_quotes
                WHERE symbol = 'SPY'
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            previous_close AS (
                SELECT close as prev_close
                FROM underlying_quotes
                WHERE symbol = 'SPY'
                  AND timestamp < (SELECT DATE_TRUNC('day', timestamp AT TIME ZONE 'America/New_York')
                                   FROM latest_price)
                ORDER BY timestamp DESC
                LIMIT 1
            )
            SELECT
                lp.current_price,
                pc.prev_close,
                (lp.current_price - pc.prev_close) as change,
                ((lp.current_price - pc.prev_close) / pc.prev_close * 100) as percent_change
            FROM latest_price lp, previous_close pc
        """)

        result = cursor.fetchone()
        cursor.close()

        if result:
            return jsonify({
                'current_price': float(result['current_price'] or 0),
                'prev_close': float(result['prev_close'] or 0),
                'change': float(result['change'] or 0),
                'percent_change': float(result['percent_change'] or 0)
            })
        else:
            return jsonify({'change': 0, 'percent_change': 0})

    except Exception as e:
        print(f"Error in spy-change: {e}")
        traceback.print_exc()
        return jsonify({'change': 0, 'percent_change': 0})
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/logo')
def get_logo():
    try:
        return send_from_directory('/opt/zerogex/monitoring', 'ZeroGEX.png')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("Starting Flask app on port 8080...")
    print(f"Dashboard directory: {DASHBOARD_DIR}")
    print(f"Metrics file: {METRICS_FILE}")
    print(f"Credentials file: {CREDS_FILE}")

    # Initialize connection pool on startup
    init_db_pool()

    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)
