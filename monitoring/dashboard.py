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
import time
from functools import wraps

app = Flask(__name__)
METRICS_FILE = Path("/data/monitoring/current_metrics.json")
DASHBOARD_DIR = Path("/opt/zerogex/monitoring")
CREDS_FILE = Path.home() / ".zerogex_db_creds"
_query_cache = {}

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
                    maxconn=3,
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

def cache_query(ttl_seconds=30):
    """Cache query results for ttl_seconds"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}:{args}:{kwargs}"
            now = time.time()

            if cache_key in _query_cache:
                result, timestamp = _query_cache[cache_key]
                if now - timestamp < ttl_seconds:
                    return result

            result = func(*args, **kwargs)
            _query_cache[cache_key] = (result, now)
            return result
        return wrapper
    return decorator

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

        if actual_table == "options_quotes":
            cursor.execute(f"""
                SELECT * FROM options_quotes
                ORDER BY last_updated DESC
                LIMIT 100
            """)

        else:
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

@app.route('/api/ingestion-history')
@cache_query(ttl_seconds=30) # Add caching
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

        # Get the last record of each hour to calculate differences
        cursor.execute("""
            WITH hourly_last_values AS (
                SELECT DISTINCT ON (date_trunc('hour', timestamp AT TIME ZONE 'America/New_York'))
                    date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') as hour,
                    timestamp,
                    records_ingested,
                    error_count
                FROM ingestion_metrics
                WHERE timestamp > NOW() - INTERVAL '48 hours'
                ORDER BY date_trunc('hour', timestamp AT TIME ZONE 'America/New_York'), timestamp DESC
            ),
            hourly_differences AS (
                SELECT
                    hour,
                    records_ingested - LAG(records_ingested, 1, 0) OVER (ORDER BY hour) as records_this_hour,
                    error_count - LAG(error_count, 1, 0) OVER (ORDER BY hour) as errors_this_hour
                FROM hourly_last_values
            )
            SELECT
                hour,
                GREATEST(records_this_hour, 0) as records_ingested,
                GREATEST(errors_this_hour, 0) as error_count
            FROM hourly_differences
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
@cache_query(ttl_seconds=30) # Add caching
def get_uptime_history():
    """Get service uptime history for exactly 48 hours with hourly buckets"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")
        cursor.execute("SET statement_timeout = '3s'")

        # Generate exactly 48 hours of hourly buckets
        cursor.execute("""
            WITH RECURSIVE hour_series AS (
                -- Start from 48 hours ago, rounded to the hour
                SELECT date_trunc('hour', NOW() - INTERVAL '48 hours') AS hour_bucket
                UNION ALL
                SELECT hour_bucket + INTERVAL '1 hour'
                FROM hour_series
                WHERE hour_bucket < date_trunc('hour', NOW())
            ),
            hourly_uptime AS (
                SELECT
                    date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') as hour,
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN is_up = 1 THEN 1 ELSE 0 END) as up_checks
                FROM service_uptime_checks
                WHERE service_name = 'gex-ingestion'
                  AND timestamp > NOW() - INTERVAL '48 hours'
                GROUP BY date_trunc('hour', timestamp AT TIME ZONE 'America/New_York')
            )
            SELECT
                hs.hour_bucket as hour,
                COALESCE(hu.up_checks, 0) as up_checks,
                COALESCE(hu.total_checks, 0) as total_checks,
                CASE
                    WHEN COALESCE(hu.total_checks, 0) = 0 THEN 0
                    ELSE ROUND((COALESCE(hu.up_checks, 0)::numeric / hu.total_checks * 100), 1)
                END as uptime_percent
            FROM hour_series hs
            LEFT JOIN hourly_uptime hu ON hs.hour_bucket = hu.hour
            ORDER BY hs.hour_bucket ASC
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
                'uptime_percent': float(row['uptime_percent'] or 0),
                'up_checks': int(row['up_checks'] or 0),
                'total_checks': int(row['total_checks'] or 0)
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in uptime-history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/logo')
def get_logo():
    try:
        return send_from_directory('/opt/zerogex/monitoring', 'Dark_Full.png')
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
