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
import traceback

app = Flask(__name__)
METRICS_FILE = Path("/home/ubuntu/monitoring/current_metrics.json")
DASHBOARD_DIR = Path("/opt/zerogex/monitoring")
CREDS_FILE = Path.home() / ".zerogex_db_creds"

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

@app.route('/')
def dashboard():
    try:
        return send_from_directory(DASHBOARD_DIR, 'dashboard.html')
    except Exception as e:
        print(f"Error serving dashboard: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics')
def get_metrics():
    try:
        if METRICS_FILE.exists():
            with open(METRICS_FILE) as f:
                return jsonify(json.load(f))
        return jsonify({'error': 'Metrics file not found'}), 404
    except Exception as e:
        print(f"Error getting metrics: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

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
        print(f"Error getting table data for {table_name}: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/spy-history')
def get_spy_history():
    """Get SPY price history with open interest data for charts"""
    conn = None
    cursor = None
    try:
        db_config = load_db_config()
        if not db_config:
            print("DB config not found")
            return jsonify({'error': 'Database config not found'}), 500

        print("Connecting to database...")
        conn = psycopg2.connect(**db_config, connect_timeout=5)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        print("Fetching SPY aggregated data (5-min buckets)...")
        # Aggregate to 5-minute buckets to reduce memory - only last 48 hours
        cursor.execute("""
            WITH five_min_buckets AS (
                SELECT
                    date_trunc('hour', timestamp) +
                    (floor(EXTRACT(minute FROM timestamp) / 5) * INTERVAL '5 minutes') as bucket_time,
                    (array_agg(open ORDER BY timestamp))[1] as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (array_agg(close ORDER BY timestamp DESC))[1] as close,
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
            LIMIT 1000
        """)
        price_data = cursor.fetchall()
        print(f"Found {len(price_data)} 5-min buckets")

        print("Fetching aggregated OI data...")
        # Aggregate OI to 5-minute buckets
        cursor.execute("""
            WITH five_min_oi AS (
                SELECT
                    date_trunc('hour', timestamp) +
                    (floor(EXTRACT(minute FROM timestamp) / 5) * INTERVAL '5 minutes') as bucket_time,
                    SUM(CASE WHEN option_type = 'call' THEN COALESCE(open_interest, 0) ELSE 0 END) as call_oi,
                    SUM(CASE WHEN option_type = 'put' THEN COALESCE(open_interest, 0) ELSE 0 END) as put_oi
                FROM options_quotes
                WHERE symbol LIKE 'SPY%'
                  AND timestamp > NOW() - INTERVAL '48 hours'
                GROUP BY bucket_time
            )
            SELECT * FROM five_min_oi
            ORDER BY bucket_time ASC
            LIMIT 1000
        """)
        oi_data = cursor.fetchall()
        print(f"Found {len(oi_data)} OI buckets")

        cursor.close()
        conn.close()

        # Create OI map - more memory efficient
        oi_map = {row['bucket_time']: {
            'call_oi': int(row['call_oi'] or 0),
            'put_oi': int(row['put_oi'] or 0)
        } for row in oi_data}

        print(f"Created OI map with {len(oi_map)} entries")

        # Combine data - limit to 576 points (48 hours * 12 per hour)
        result = []
        for row in price_data[:576]:  # Hard limit
            ts = row['bucket_time']
            if not ts:
                continue

            oi_info = oi_map.get(ts, {'call_oi': 0, 'put_oi': 0})

            result.append({
                'timestamp': ts.strftime('%Y-%m-%dT%H:%M:%S'),
                'open': float(row['open'] or row['close'] or 0),
                'high': float(row['high'] or row['close'] or 0),
                'low': float(row['low'] or row['close'] or 0),
                'close': float(row['close'] or 0),
                'volume': int(row['volume'] or 0),
                'up_volume': int(row['up_volume'] or 0),
                'down_volume': int(row['down_volume'] or 0),
                'call_oi': oi_info['call_oi'],
                'put_oi': oi_info['put_oi']
            })

        print(f"Returning {len(result)} combined data points")
        return jsonify(result)

    except psycopg2.OperationalError as e:
        print(f"Database connection error in spy-history: {e}")
        return jsonify({'error': f'Database connection failed: {str(e)}'}), 500
    except Exception as e:
        print(f"Error in spy-history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

@app.route('/api/ingestion-history')
def get_ingestion_history():
    """Get ingestion metrics history for charts"""
    conn = None
    cursor = None
    try:
        db_config = load_db_config()
        if not db_config:
            return jsonify({'error': 'Database config not found'}), 500

        conn = psycopg2.connect(**db_config, connect_timeout=5)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Aggregate to hourly to reduce memory
        cursor.execute("""
            SELECT
                date_trunc('hour', timestamp) as hour,
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
        conn.close()

        result = [{'timestamp': row['hour'].strftime('%Y-%m-%dT%H:%M:%S'),
                   'records_ingested': int(row['records_ingested']),
                   'error_count': int(row['error_count'])} for row in rows]

        return jsonify(result)

    except Exception as e:
        print(f"Error in ingestion-history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

@app.route('/api/uptime-history')
def get_uptime_history():
    """Get service uptime history based on actual service checks"""
    conn = None
    cursor = None
    try:
        db_config = load_db_config()
        if not db_config:
            return jsonify({'error': 'Database config not found'}), 500

        conn = psycopg2.connect(**db_config, connect_timeout=5)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Calculate uptime - simpler version
        cursor.execute("""
            WITH hourly_checks AS (
                SELECT
                    date_trunc('hour', timestamp) as hour,
                    AVG(CASE WHEN is_up = 1 THEN 100.0 ELSE 0.0 END) as uptime_percent
                FROM service_uptime_checks
                WHERE service_name = 'gex-ingestion'
                  AND timestamp > NOW() - INTERVAL '48 hours'
                GROUP BY date_trunc('hour', timestamp)
            )
            SELECT
                EXTRACT(EPOCH FROM hour)::bigint * 1000 as timestamp_ms,
                ROUND(uptime_percent::numeric, 1) as uptime_percent
            FROM hourly_checks
            ORDER BY hour ASC
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        result = [{'timestamp': row['timestamp_ms'], 
                   'uptime_percent': float(row['uptime_percent'] or 0)} for row in rows]

        return jsonify(result)

    except Exception as e:
        print(f"Error in uptime-history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

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
    app.run(host='0.0.0.0', port=8080, debug=True)
