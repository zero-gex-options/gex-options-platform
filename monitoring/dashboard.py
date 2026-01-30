#!/usr/bin/env python3
"""
ZeroGEX Monitoring Web Dashboard
Simple Flask app to display monitoring metrics
"""

from flask import Flask, jsonify, send_from_directory
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
METRICS_FILE = Path("/home/ubuntu/monitoring/current_metrics.json")
DASHBOARD_DIR = Path("/opt/zerogex/monitoring")
CREDS_FILE = Path.home() / ".zerogex_db_creds"

def load_db_config():
    """Load database configuration from ~/.zerogex_db_creds"""
    if not CREDS_FILE.exists():
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

@app.route('/')
def dashboard():
    return send_from_directory(DASHBOARD_DIR, 'dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    try:
        if METRICS_FILE.exists():
            with open(METRICS_FILE) as f:
                return jsonify(json.load(f))
        return jsonify({'error': 'Metrics file not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/table/<table_name>')
def get_table_data(table_name):
    """Get recent 100 rows from a database table"""
    # Whitelist allowed tables
    allowed_tables = {
        'options_quotes': 'options_quotes',
        'underlying_quotes': 'underlying_quotes',
        'gex_metrics': 'gex_metrics',
        'ingestion_metrics': 'ingestion_metrics'
    }

    if table_name not in allowed_tables:
        return jsonify({'error': 'Invalid table name'}), 400

    actual_table = allowed_tables[table_name]

    try:
        db_config = load_db_config()
        if not db_config:
            return jsonify({'error': 'Database config not found'}), 500

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"""
            SELECT * FROM {actual_table}
            ORDER BY timestamp DESC
            LIMIT 100
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/spy-history')
def get_spy_history():
    """Get SPY price history for charts"""
    try:
        db_config = load_db_config()
        if not db_config:
            return jsonify({'error': 'Database config not found'}), 500

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get last 48 hours of SPY data with up/down volume
        cursor.execute("""
            SELECT timestamp, close as price, total_volume as volume,
                   up_volume, down_volume
            FROM underlying_quotes
            WHERE symbol = 'SPY'
              AND timestamp > NOW() - INTERVAL '48 hours'
            ORDER BY timestamp ASC
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/ingestion-history')
def get_ingestion_history():
    """Get ingestion metrics history for charts"""
    try:
        db_config = load_db_config()
        if not db_config:
            return jsonify({'error': 'Database config not found'}), 500

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get last 48 hours of ingestion metrics
        cursor.execute("""
            SELECT timestamp, records_ingested, error_count
            FROM ingestion_metrics
            WHERE timestamp > NOW() - INTERVAL '48 hours'
            ORDER BY timestamp ASC
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/uptime-history')
def get_uptime_history():
    """Get service uptime history based on actual service checks"""
    try:
        db_config = load_db_config()
        if not db_config:
            return jsonify({'error': 'Database config not found'}), 500

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Calculate uptime based on service check data
        # Group into 15-minute buckets, then aggregate by hour
        cursor.execute("""
            WITH fifteen_min_buckets AS (
                SELECT
                    date_trunc('hour', timestamp) +
                    (EXTRACT(minute FROM timestamp)::int / 15) * INTERVAL '15 minutes' as interval_time,
                    -- If ANY check in this 15-min interval shows service up, count it as up
                    MAX(is_up) as interval_up
                FROM service_uptime_checks
                WHERE service_name = 'gex-ingestion'
                  AND timestamp > NOW() - INTERVAL '48 hours'
                GROUP BY interval_time
            ),
            hourly_uptime AS (
                SELECT
                    date_trunc('hour', interval_time) as hour,
                    COUNT(*) as intervals_up,
                    (COUNT(*)::float / 4.0) * 100 as uptime_percent
                FROM fifteen_min_buckets
                WHERE interval_up = 1  -- Only count intervals where service was up
                GROUP BY date_trunc('hour', interval_time)
            ),
            all_hours AS (
                SELECT generate_series(
                    date_trunc('hour', NOW() - INTERVAL '48 hours'),
                    date_trunc('hour', NOW()),
                    '1 hour'::interval
                ) as hour
            )
            SELECT
                EXTRACT(EPOCH FROM ah.hour)::bigint * 1000 as timestamp_ms,
                COALESCE(ROUND(hu.uptime_percent::numeric, 1), 0) as uptime_percent
            FROM all_hours ah
            LEFT JOIN hourly_uptime hu ON ah.hour = hu.hour
            ORDER BY ah.hour ASC
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify([{'timestamp': row['timestamp_ms'], 'uptime_percent': float(row['uptime_percent'])} for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/logo')
def get_logo():
    return send_from_directory('/opt/zerogex/monitoring', 'ZeroGEX.png')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
