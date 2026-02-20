#!/usr/bin/env python3
"""
ZeroGEX Customer Dashboard Backend
Flask app serving GEX analytics and insights
"""

from flask import Flask, jsonify, send_from_directory
import json
from pathlib import Path
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import traceback
from datetime import datetime, date, timedelta, time as dt_time
import pytz
import time
from functools import wraps

app = Flask(__name__)
DASHBOARD_DIR = Path("/opt/zerogex/frontend/templates")
CREDS_FILE = Path.home() / ".zerogex_db_creds"
_query_cache = {}

# Global connection pool
db_pool = None

def get_target_expiration():
    """
    Get the appropriate expiration date based on current market time.
    Same logic as streaming_ingestion_engine.py

    For 0DTE trading:
    - Before 4:00 PM ET: Use today's date
    - After 4:00 PM ET: Use next trading day's date

    Returns:
        date: Target expiration date
    """
    now_et = datetime.now(pytz.timezone('America/New_York'))
    current_date = now_et.date()

    # If after 4:00 PM ET (options expiration time), move to next day
    if now_et.time() >= dt_time(16, 0):
        # Move to next day
        next_day = current_date + timedelta(days=1)

        # Skip weekends (Saturday=5, Sunday=6)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)

        return next_day

    return current_date

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
def index():
    """Serve homepage dashboard"""
    try:
        return send_from_directory(DASHBOARD_DIR, 'index.html')
    except Exception as e:
        print(f"Error serving index: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/about')
def about_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'about.html')
    except Exception as e:
        print(f"Error serving about page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/gamma')
def gamma_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'gamma_page.html')
    except Exception as e:
        print(f"Error serving gamma page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/put-call')
def put_call_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'put_call_page.html')
    except Exception as e:
        print(f"Error serving put/call page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/flows')
def flows_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'flows_page.html')
    except Exception as e:
        print(f"Error serving flows page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/spy-price')
def spy_price_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'spy_price_page.html')
    except Exception as e:
        print(f"Error serving SPY price page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/market-bias')
def market_bias_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'market_bias_page.html')
    except Exception as e:
        print(f"Error serving market bias page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/max-pain')
def max_pain_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'max_pain_page.html')
    except Exception as e:
        print(f"Error serving max pain page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/_navigation.html')
def serve_navigation():
    """Serve navigation HTML fragment"""
    try:
        return send_from_directory(DASHBOARD_DIR, '_navigation.html')
    except Exception as e:
        print(f"Error serving navigation: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/spy')
def spy_page():
    try:
        return send_from_directory(DASHBOARD_DIR, 'spy_frontend.html')
    except Exception as e:
        print(f"Error serving SPY page: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/gex/current')
@cache_query(ttl_seconds=10)
def get_current_gex():
    """Get current GEX metrics - uses dynamic expiration"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get target expiration based on time of day
        target_exp = get_target_expiration()

        # Get latest GEX metrics for the target expiration
        cursor.execute("""
            SELECT 
                timestamp,
                symbol,
                expiration,
                underlying_price,
                total_gamma_exposure,
                call_gamma,
                put_gamma,
                net_gex,
                call_volume,
                put_volume,
                call_oi,
                put_oi,
                total_contracts,
                max_gamma_strike,
                max_gamma_value,
                gamma_flip_point,
                max_pain,
                put_call_ratio,
                vanna_exposure,
                charm_exposure
            FROM gex_metrics
            WHERE symbol = 'SPY'
                AND DATE(expiration) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (target_exp,))

        row = cursor.fetchone()
        cursor.close()

        if not row:
            return jsonify({'error': 'No GEX data available'}), 404

        # Convert to millions and add derived fields
        result = dict(row)
        result['total_gex_millions'] = result['total_gamma_exposure'] / 1e6
        result['net_gex_millions'] = result['net_gex'] / 1e6
        result['call_gamma_millions'] = result['call_gamma'] / 1e6
        result['put_gamma_millions'] = result['put_gamma'] / 1e6
        result['gamma_regime'] = 'Positive (Stabilizing)' if result['net_gex'] > 0 else 'Negative (Destabilizing)'
        result['regime_color'] = '#10b981' if result['net_gex'] > 0 else '#ef4444'

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_current_gex: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/gex/history')
@cache_query(ttl_seconds=30)
def get_gex_history():
    """Get historical GEX metrics"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        # Get last 48 hours of GEX data
        cursor.execute("""
            SELECT 
                timestamp,
                underlying_price,
                total_gamma_exposure / 1e6 as total_gex_millions,
                net_gex / 1e6 as net_gex_millions,
                call_gamma / 1e6 as call_gamma_millions,
                put_gamma / 1e6 as put_gamma_millions,
                max_gamma_strike,
                gamma_flip_point,
                put_call_ratio
            FROM gex_metrics
            WHERE symbol = 'SPY'
                AND timestamp > NOW() - INTERVAL '5 days'
            ORDER BY timestamp ASC
        """)

        rows = cursor.fetchall()
        cursor.close()

        eastern = pytz.timezone('America/New_York')
        result = []
        for row in rows:
            ts = row['timestamp']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            data = dict(row)
            data['timestamp'] = ts.isoformat()
            result.append(data)

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_gex_history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/gex/strike-profile')
@cache_query(ttl_seconds=10)
def get_strike_profile():
    """Get gamma exposure by strike price - uses dynamic expiration"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get latest underlying price
        cursor.execute("""
            SELECT close as spot_price
            FROM underlying_quotes
            WHERE symbol = 'SPY'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        spot_result = cursor.fetchone()
        spot_price = float(spot_result['spot_price']) if spot_result else 600.0

        # Get target expiration based on time of day
        target_exp = get_target_expiration()

        # Get options data for the target expiration
        cursor.execute("""
            SELECT DISTINCT ON (strike, option_type)
                strike,
                option_type,
                gamma,
                open_interest,
                underlying_price
            FROM options_quotes
            WHERE symbol LIKE 'SPY%%'
                AND DATE(expiration) = %s
                AND last_updated > NOW() - INTERVAL '4 hours'
                AND gamma IS NOT NULL
                AND gamma > 0
            ORDER BY strike, option_type, last_updated DESC
        """, (target_exp,))

        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            return jsonify({'error': 'No options data available'}), 404

        # Calculate gamma exposure by strike
        strike_data = {}
        for row in rows:
            strike = float(row['strike'])
            opt_type = row['option_type']
            gamma = float(row['gamma'])
            oi = int(row['open_interest'])

            # Gamma exposure = gamma * OI * 100 * spot price
            gamma_exp = gamma * oi * 100 * spot_price

            if strike not in strike_data:
                strike_data[strike] = {
                    'strike': strike,
                    'call_gamma': 0,
                    'put_gamma': 0,
                    'call_oi': 0,
                    'put_oi': 0
                }

            if opt_type == 'call':
                strike_data[strike]['call_gamma'] += gamma_exp
                strike_data[strike]['call_oi'] += oi
            else:
                strike_data[strike]['put_gamma'] += gamma_exp
                strike_data[strike]['put_oi'] += oi

        # Convert to list and add derived fields
        result = []
        for strike, data in sorted(strike_data.items()):
            data['total_gamma'] = data['call_gamma'] + data['put_gamma']
            data['net_gamma'] = data['call_gamma'] - data['put_gamma']
            data['total_gamma_millions'] = data['total_gamma'] / 1e6
            data['net_gamma_millions'] = data['net_gamma'] / 1e6
            data['call_gamma_millions'] = data['call_gamma'] / 1e6
            data['put_gamma_millions'] = data['put_gamma'] / 1e6
            result.append(data)

        return jsonify({
            'spot_price': spot_price,
            'expiration': target_exp.isoformat(),
            'strikes': result
        })

    except Exception as e:
        print(f"Error in get_strike_profile: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/gex/regime-changes')
@cache_query(ttl_seconds=60)
def get_regime_changes():
    """Get gamma regime changes over time"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        # Get regime changes (when net_gex crosses zero)
        cursor.execute("""
            WITH gex_with_regime AS (
                SELECT 
                    timestamp,
                    underlying_price,
                    net_gex,
                    CASE WHEN net_gex > 0 THEN 'positive' ELSE 'negative' END as regime
                FROM gex_metrics
                WHERE symbol = 'SPY'
                    AND timestamp > NOW() - INTERVAL '7 days'
                ORDER BY timestamp ASC
            ),
            regime_changes AS (
                SELECT 
                    timestamp,
                    underlying_price,
                    net_gex,
                    regime,
                    LAG(regime) OVER (ORDER BY timestamp) as prev_regime
                FROM gex_with_regime
            )
            SELECT 
                timestamp,
                underlying_price,
                net_gex / 1e6 as net_gex_millions,
                prev_regime as from_regime,
                regime as to_regime
            FROM regime_changes
            WHERE regime != prev_regime
            ORDER BY timestamp DESC
            LIMIT 20
        """)

        rows = cursor.fetchall()
        cursor.close()

        eastern = pytz.timezone('America/New_York')
        result = []
        for row in rows:
            ts = row['timestamp']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            data = dict(row)
            data['timestamp'] = ts.isoformat()
            result.append(data)

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_regime_changes: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/gex/key-levels')
@cache_query(ttl_seconds=30)
def get_key_levels():
    """Get key support/resistance levels based on gamma - uses dynamic expiration"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get spot price
        cursor.execute("""
            SELECT close as spot_price
            FROM underlying_quotes
            WHERE symbol = 'SPY'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        spot_result = cursor.fetchone()
        spot_price = float(spot_result['spot_price']) if spot_result else 600.0

        # Get target expiration based on time of day
        target_exp = get_target_expiration()

        # Get options with significant gamma
        cursor.execute("""
            SELECT DISTINCT ON (strike, option_type)
                strike,
                option_type,
                gamma,
                open_interest
            FROM options_quotes
            WHERE symbol LIKE 'SPY%%'
                AND DATE(expiration) = %s
                AND last_updated > NOW() - INTERVAL '4 hours'
                AND gamma IS NOT NULL
            ORDER BY strike, option_type, last_updated DESC
        """, (target_exp,))

        rows = cursor.fetchall()
        cursor.close()

        # Calculate gamma by strike
        strike_gamma = {}
        for row in rows:
            strike = float(row['strike'])
            opt_type = row['option_type']
            gamma = float(row['gamma'])
            oi = int(row['open_interest'])

            gamma_exp = gamma * oi * 100 * spot_price / 1e6  # In millions

            if strike not in strike_gamma:
                strike_gamma[strike] = {'call': 0, 'put': 0}

            if opt_type == 'call':
                strike_gamma[strike]['call'] += gamma_exp
            else:
                strike_gamma[strike]['put'] += gamma_exp

        # Find significant levels (threshold: 50M gamma)
        threshold = 50.0
        support_levels = []
        resistance_levels = []

        for strike, gamma in strike_gamma.items():
            if gamma['put'] >= threshold and strike <= spot_price:
                support_levels.append({
                    'strike': strike,
                    'gamma_millions': gamma['put']
                })

            if gamma['call'] >= threshold and strike >= spot_price:
                resistance_levels.append({
                    'strike': strike,
                    'gamma_millions': gamma['call']
                })

        return jsonify({
            'spot_price': spot_price,
            'expiration': target_exp.isoformat(),
            'support': sorted(support_levels, key=lambda x: x['strike'], reverse=True)[:5],
            'resistance': sorted(resistance_levels, key=lambda x: x['strike'])[:5]
        })

    except Exception as e:
        print(f"Error in get_key_levels: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/gex/put-call-history')
@cache_query(ttl_seconds=30)
def get_put_call_history():
    """Get historical put/call ratio data"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        cursor.execute("""
            SELECT 
                timestamp,
                put_call_ratio
            FROM gex_metrics
            WHERE symbol = 'SPY'
                AND timestamp > NOW() - INTERVAL '48 hours'
            ORDER BY timestamp ASC
        """)

        rows = cursor.fetchall()
        cursor.close()

        import pytz
        eastern = pytz.timezone('America/New_York')
        result = []
        for row in rows:
            ts = row['timestamp']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            result.append({
                'timestamp': ts.isoformat(),
                'put_call_ratio': float(row['put_call_ratio']) if row['put_call_ratio'] else 0
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_put_call_history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/flows/history')
@cache_query(ttl_seconds=30)
def get_flows_history():
    """Get historical option flow data from option_flow_metrics table"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        # Query the option_flow_metrics table - last 48 hours
        cursor.execute("""
            SELECT 
                timestamp,
                symbol,
                option_type,
                total_premium,
                delta_weighted_volume,
                net_delta_exposure,
                total_notional,
                buy_volume,
                sell_volume,
                net_flow,
                avg_underlying_price
            FROM option_flow_metrics
            WHERE symbol = 'SPY'
                AND timestamp > NOW() - INTERVAL '48 hours'
            ORDER BY timestamp ASC
        """)

        rows = cursor.fetchall()
        cursor.close()

        import pytz
        eastern = pytz.timezone('America/New_York')

        # Aggregate by timestamp
        buckets = {}
        cumulative_delta_flow = 0  # Track cumulative delta flow

        for row in rows:
            ts = row['timestamp']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)
            ts_str = ts.isoformat()

            if ts_str not in buckets:
                buckets[ts_str] = {
                    'timestamp': ts_str,
                    'premium_spent': 0,
                    'delta_weighted_flow': 0,  # This will be cumulative
                    'call_notional': 0,
                    'put_notional': 0
                }

            # Premium is per-bucket (sum calls + puts)
            buckets[ts_str]['premium_spent'] += float(row['total_premium'] or 0)

            # Delta flow is cumulative across the entire time series
            cumulative_delta_flow += float(row['delta_weighted_volume'] or 0)
            buckets[ts_str]['delta_weighted_flow'] = cumulative_delta_flow

            # Notional values are per-bucket, separate for calls and puts
            if row['option_type'] == 'call':
                buckets[ts_str]['call_notional'] += float(row['total_notional'] or 0)
            else:
                buckets[ts_str]['put_notional'] += float(row['total_notional'] or 0)

        # Convert to sorted list
        result = sorted(buckets.values(), key=lambda x: x['timestamp'])

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_flows_history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/market/SPY/latest')
def get_spy_latest():
    """Get only the most recent SPY bar - no cache, for real-time banner updates"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT
                timestamp,
                actual_time,
                close,
                open,
                high,
                low,
                total_volume as volume,
                up_volume,
                down_volume
            FROM underlying_quotes
            WHERE symbol = 'SPY'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        if not row:
            return jsonify({'error': 'No data'}), 404
        result = dict(row)
        # Serialize datetime fields
        import pytz
        eastern = pytz.timezone('America/New_York')
        for key in ['timestamp', 'actual_time']:
            if result.get(key):
                ts = result[key]
                if hasattr(ts, 'tzinfo'):
                    if ts.tzinfo is None:
                        ts = eastern.localize(ts)
                    else:
                        ts = ts.astimezone(eastern)
                    result[key] = ts.isoformat()
        return jsonify(result)
    except Exception as e:
        print(f"Error in get_spy_latest: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/market/SPY/current')
@cache_query(ttl_seconds=5)
def get_spy_current():
    """Get current SPY market data"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT 
                actual_time,
                symbol,
                close as last_price,
                open,
                high,
                low,
                total_volume as volume,
                up_volume,
                down_volume
            FROM underlying_quotes
            WHERE symbol = 'SPY'
            ORDER BY timestamp DESC
            LIMIT 1
        """)

        row = cursor.fetchone()
        cursor.close()

        if not row:
            return jsonify({'error': 'No current data available'}), 404

        return jsonify(dict(row))

    except Exception as e:
        print(f"Error in get_spy_current: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/market/SPY/history')
@cache_query(ttl_seconds=30)
def get_spy_market_history():
    """Get SPY market history for charts - last 384 5-minute bars"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        cursor.execute("""
            WITH five_min_buckets AS (
                SELECT
                    date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') +
                    (floor((EXTRACT(minute FROM timestamp AT TIME ZONE 'America/New_York') - 1) / 5) * INTERVAL '5 minutes') as bucket_time,
                    (array_agg(open ORDER BY timestamp))[1] as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (array_agg(close ORDER BY timestamp DESC))[1] as close,
                    MAX(timestamp) as actual_timestamp,
                    MAX(actual_time) as actual_time,
                    SUM(COALESCE(total_volume, 0)) as volume,
                    SUM(COALESCE(up_volume, 0)) as up_volume,
                    SUM(COALESCE(down_volume, 0)) as down_volume
                FROM underlying_quotes
                WHERE symbol = 'SPY'
                GROUP BY bucket_time
                ORDER BY bucket_time DESC
                LIMIT 384
            )
            SELECT * FROM five_min_buckets
            ORDER BY bucket_time DESC
        """)

        rows = cursor.fetchall()
        cursor.close()

        # Reverse to get chronological order
        rows = list(reversed(rows)) if rows else []

        import pytz
        eastern = pytz.timezone('America/New_York')
        result = []
        for row in rows:
            ts = row['bucket_time']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            actual_ts = row['actual_timestamp']
            if actual_ts:
                if actual_ts.tzinfo is None:
                    actual_ts_display = eastern.localize(actual_ts)
                else:
                    actual_ts_display = actual_ts.astimezone(eastern)
            else:
                actual_ts_display = ts

            result.append({
                'timestamp': ts.isoformat(),
                'actual_timestamp': actual_ts_display.isoformat(),
                'actual_time': row['actual_time'].isoformat() if row['actual_time'] else None,
                'open': float(row['open'] or row['close'] or 0),
                'high': float(row['high'] or row['close'] or 0),
                'low': float(row['low'] or row['close'] or 0),
                'close': float(row['close'] or 0),
                'volume': int(row['volume'] or 0),
                'up_volume': int(row['up_volume'] or 0),
                'down_volume': int(row['down_volume'] or 0)
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_spy_market_history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/spy-48hr-range')
@cache_query(ttl_seconds=10)
def get_spy_48hr_range():
    """Get SPY range from last 384 5-minute bars and today's cumulative volume"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        # Get range from last 384 5-minute buckets
        cursor.execute("""
            WITH five_min_buckets AS (
                SELECT
                    date_trunc('hour', timestamp AT TIME ZONE 'America/New_York') +
                    (floor((EXTRACT(minute FROM timestamp AT TIME ZONE 'America/New_York') - 1) / 5) * INTERVAL '5 minutes') as bucket_time,
                    MAX(high) as high,
                    MIN(low) as low
                FROM underlying_quotes
                WHERE symbol = 'SPY'
                GROUP BY bucket_time
                ORDER BY bucket_time DESC
                LIMIT 384
            )
            SELECT
                MIN(low) as range_low,
                MAX(high) as range_high
            FROM five_min_buckets
        """)
        range_result = cursor.fetchone()

        # Get today's cumulative volume since 4:00 AM ET
        cursor.execute("""
            SELECT
                SUM(COALESCE(total_volume, 0)) as total_volume,
                SUM(COALESCE(up_volume, 0)) as up_volume,
                SUM(COALESCE(down_volume, 0)) as down_volume
            FROM underlying_quotes
            WHERE symbol = 'SPY'
                AND timestamp >= DATE_TRUNC('day', NOW() AT TIME ZONE 'America/New_York')
                    + INTERVAL '4 hours'
        """)
        volume_result = cursor.fetchone()

        cursor.close()

        return jsonify({
            'range_low': float(range_result['range_low']) if range_result and range_result['range_low'] else 0,
            'range_high': float(range_result['range_high']) if range_result and range_result['range_high'] else 0,
            'total_volume': int(volume_result['total_volume']) if volume_result and volume_result['total_volume'] else 0,
            'up_volume': int(volume_result['up_volume']) if volume_result and volume_result['up_volume'] else 0,
            'down_volume': int(volume_result['down_volume']) if volume_result and volume_result['down_volume'] else 0
        })

    except Exception as e:
        print(f"Error in get_spy_48hr_range: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/market/bias/history')
@cache_query(ttl_seconds=30)
def get_bias_history():
    """Get historical market bias data"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        cursor.execute("""
            WITH gex_data AS (
                SELECT 
                    timestamp,
                    net_gex,
                    gamma_flip_point,
                    underlying_price,
                    put_call_ratio,
                    max_gamma_strike
                FROM gex_metrics
                WHERE symbol = 'SPY'
                    AND timestamp > NOW() - INTERVAL '48 hours'
            )
            SELECT 
                timestamp,
                net_gex,
                gamma_flip_point,
                underlying_price,
                put_call_ratio,
                max_gamma_strike,
                CASE 
                    WHEN net_gex > 0 AND underlying_price > COALESCE(gamma_flip_point, underlying_price) THEN 'Bullish'
                    WHEN net_gex < 0 AND underlying_price < COALESCE(gamma_flip_point, underlying_price) THEN 'Bearish'
                    ELSE 'Neutral'
                END as market_bias
            FROM gex_data
            ORDER BY timestamp;
        """)

        rows = cursor.fetchall()
        cursor.close()

        import pytz
        eastern = pytz.timezone('America/New_York')
        result = []
        for row in rows:
            ts = row['timestamp']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            result.append({
                'timestamp': ts.isoformat(),
                'market_bias': row['market_bias'],
                'net_gex': float(row['net_gex']) if row['net_gex'] else 0,
                'gamma_flip_point': float(row['gamma_flip_point']) if row['gamma_flip_point'] else None,
                'underlying_price': float(row['underlying_price']) if row['underlying_price'] else 0,
                'put_call_ratio': float(row['put_call_ratio']) if row['put_call_ratio'] else 0,
                'max_gamma_strike': float(row['max_gamma_strike']) if row['max_gamma_strike'] else 0
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_bias_history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/market/bias/current')
@cache_query(ttl_seconds=10)
def get_current_bias_score():
    """Get current market bias with calculated score - uses dynamic expiration"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get target expiration based on time of day
        target_exp = get_target_expiration()

        # Get latest GEX metrics for target expiration
        cursor.execute("""
            SELECT
                timestamp,
                expiration,
                net_gex,
                underlying_price,
                gamma_flip_point,
                put_call_ratio,
                max_gamma_strike
            FROM gex_metrics
            WHERE symbol = 'SPY'
                AND DATE(expiration) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (target_exp,))

        row = cursor.fetchone()
        cursor.close()

        if not row:
            return jsonify({'error': 'No data available'}), 404

        data = dict(row)

        # Calculate bias score (same logic as frontend)
        score = 0

        # Net GEX contribution (-30 to +30)
        if data['net_gex']:
            score += min(30, max(-30, (data['net_gex'] / 1e9) * 30))

        # Flip point contribution (-25 to +25)
        if data['gamma_flip_point'] and data['underlying_price']:
            flip_distance = ((data['underlying_price'] - data['gamma_flip_point']) / data['underlying_price']) * 100
            score += max(-25, min(25, flip_distance * 5))

        # Put/Call ratio contribution (-20 to +20)
        if data['put_call_ratio']:
            score += max(-20, min(20, (1 - data['put_call_ratio']) * 20))

        # Max gamma distance contribution (-15 to +15)
        if data['max_gamma_strike'] and data['underlying_price']:
            gamma_distance = ((data['underlying_price'] - data['max_gamma_strike']) / data['underlying_price']) * 100
            score += -max(-15, min(15, gamma_distance * 3))

        # Determine bias
        if score > 25:
            bias = 'BULLISH'
            css_class = 'bullish'
        elif score < -25:
            bias = 'BEARISH'
            css_class = 'bearish'
        else:
            bias = 'NEUTRAL'
            css_class = 'neutral'

        return jsonify({
            'timestamp': data['timestamp'].isoformat(),
            'expiration': data['expiration'].isoformat() if data['expiration'] else None,
            'bias': bias,
            'score': round(score, 1),
            'css_class': css_class,
            'net_gex': float(data['net_gex']) if data['net_gex'] else 0,
            'gamma_flip_point': float(data['gamma_flip_point']) if data['gamma_flip_point'] else None,
            'underlying_price': float(data['underlying_price']) if data['underlying_price'] else 0,
            'put_call_ratio': float(data['put_call_ratio']) if data['put_call_ratio'] else 0,
            'max_gamma_strike': float(data['max_gamma_strike']) if data['max_gamma_strike'] else 0
        })

    except Exception as e:
        print(f"Error in get_current_bias_score: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/market/bias/score-history')
@cache_query(ttl_seconds=30)
def get_bias_score_history():
    """Get historical market bias scores"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        cursor.execute("""
            SELECT
                timestamp,
                net_gex,
                gamma_flip_point,
                underlying_price,
                put_call_ratio,
                max_gamma_strike
            FROM gex_metrics
            WHERE symbol = 'SPY'
                AND timestamp > NOW() - INTERVAL '48 hours'
            ORDER BY timestamp ASC
        """)

        rows = cursor.fetchall()
        cursor.close()

        import pytz
        eastern = pytz.timezone('America/New_York')
        result = []

        for row in rows:
            ts = row['timestamp']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            # Calculate score for each point
            score = 0

            # Net GEX contribution
            if row['net_gex']:
                score += min(30, max(-30, (row['net_gex'] / 1e9) * 30))

            # Flip point contribution
            if row['gamma_flip_point'] and row['underlying_price']:
                flip_distance = ((row['underlying_price'] - row['gamma_flip_point']) / row['underlying_price']) * 100
                score += max(-25, min(25, flip_distance * 5))

            # Put/Call ratio contribution
            if row['put_call_ratio']:
                score += max(-20, min(20, (1 - row['put_call_ratio']) * 20))

            # Max gamma distance contribution
            if row['max_gamma_strike'] and row['underlying_price']:
                gamma_distance = ((row['underlying_price'] - row['max_gamma_strike']) / row['underlying_price']) * 100
                score += -max(-15, min(15, gamma_distance * 3))

            # Determine bias
            if score > 25:
                bias = 'BULLISH'
            elif score < -25:
                bias = 'BEARISH'
            else:
                bias = 'NEUTRAL'

            result.append({
                'timestamp': ts.isoformat(),
                'score': round(score, 1),
                'bias': bias,
                'net_gex': float(row['net_gex']) if row['net_gex'] else 0,
                'gamma_flip_point': float(row['gamma_flip_point']) if row['gamma_flip_point'] else None,
                'underlying_price': float(row['underlying_price']) if row['underlying_price'] else 0,
                'put_call_ratio': float(row['put_call_ratio']) if row['put_call_ratio'] else 0,
                'max_gamma_strike': float(row['max_gamma_strike']) if row['max_gamma_strike'] else 0
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_bias_score_history: {e}")
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

        # Try to get from monitoring cache first
        try:
            import json
            from pathlib import Path
            cache_file = Path("/data/monitoring/spy_previous_close.json")
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)
                prev_close = cache_data.get('prev_close')
            else:
                prev_close = None
        except:
            prev_close = None

        # Get current price
        cursor.execute("""
            SELECT close as current_price
            FROM underlying_quotes
            WHERE symbol = 'SPY'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        result = cursor.fetchone()

        if not result:
            return jsonify({'error': 'No current price'}), 404

        current_price = float(result['current_price'])

        # If no cache, calculate from database
        if not prev_close:
            cursor.execute("""
                WITH latest_price AS (
                    SELECT timestamp
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
                SELECT prev_close FROM previous_close
            """)
            prev_result = cursor.fetchone()
            prev_close = float(prev_result['prev_close']) if prev_result else current_price

        cursor.close()

        change = current_price - prev_close
        percent_change = (change / prev_close * 100) if prev_close > 0 else 0

        return jsonify({
            'current_price': current_price,
            'prev_close': prev_close,
            'change': change,
            'percent_change': percent_change
        })

    except Exception as e:
        print(f"Error in get_spy_change: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/max-pain/analysis')
@cache_query(ttl_seconds=10)
def get_max_pain_analysis():
    """Get max pain analysis with strike-by-strike breakdown - uses dynamic expiration"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        # Get current SPY price
        cursor.execute("""
            SELECT close as current_price
            FROM underlying_quotes
            WHERE symbol = 'SPY'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        price_result = cursor.fetchone()
        current_price = float(price_result['current_price']) if price_result else 600.0

        # Get target expiration based on time of day
        target_date = get_target_expiration()

        # Get the most recent max pain data for target expiration
        cursor.execute("""
            SELECT max_pain, timestamp, expiration
            FROM gex_metrics
            WHERE symbol = 'SPY'
                AND DATE(expiration) = %s
                AND max_pain IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
        """, (target_date,))
        max_pain_result = cursor.fetchone()

        if not max_pain_result or not max_pain_result['max_pain']:
            return jsonify({'error': 'No max pain data available'}), 404

        max_pain = float(max_pain_result['max_pain'])
        timestamp = max_pain_result['timestamp']

        # Get options data for that expiration
        cursor.execute("""
            SELECT
                strike,
                option_type,
                open_interest,
                mid,
                underlying_price
            FROM (
                SELECT
                    strike,
                    option_type,
                    open_interest,
                    mid,
                    underlying_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY strike, option_type
                        ORDER BY last_updated DESC
                    ) as rn
                FROM options_quotes
                WHERE symbol LIKE 'SPY%%'
                    AND expiration = %s
                    AND open_interest > 0
            ) latest
            WHERE rn = 1
            ORDER BY strike, option_type
        """, (target_date,))

        options_rows = cursor.fetchall()
        cursor.close()

        if not options_rows:
            return jsonify({'error': 'No options data available'}), 404

        # Build strikes_dict
        strikes_dict = {}
        for row in options_rows:
            try:
                strike = float(row['strike'])
                opt_type = row['option_type']
                oi = int(row['open_interest'])
            except Exception as e:
                continue

            if strike not in strikes_dict:
                strikes_dict[strike] = {
                    'strike': strike,
                    'call_oi': 0,
                    'put_oi': 0,
                    'call_value': 0,
                    'put_value': 0,
                    'total_value': 0
                }

            if opt_type == 'call':
                strikes_dict[strike]['call_oi'] = oi
            else:
                strikes_dict[strike]['put_oi'] = oi

        # Calculate intrinsic value at each strike
        strikes_items = list(strikes_dict.items())

        for outer_strike, outer_data in strikes_items:
            call_value = 0
            put_value = 0

            for inner_strike, inner_data in strikes_items:
                try:
                    if inner_data['call_oi'] > 0:
                        intrinsic = max(0, float(outer_strike) - float(inner_strike))
                        call_value += intrinsic * inner_data['call_oi'] * 100

                    if inner_data['put_oi'] > 0:
                        intrinsic = max(0, float(inner_strike) - float(outer_strike))
                        put_value += intrinsic * inner_data['put_oi'] * 100
                except Exception as e:
                    continue

            outer_data['call_value'] = call_value
            outer_data['put_value'] = put_value
            outer_data['total_value'] = call_value + put_value

        # Convert to sorted list
        strikes_list = sorted(strikes_dict.values(), key=lambda x: x['strike'])

        # Find total value at max pain
        max_pain_data = next((s for s in strikes_list if s['strike'] == max_pain), None)
        total_value_at_max_pain = max_pain_data['total_value'] if max_pain_data else 0

        # Format expiration display
        import pytz
        eastern = pytz.timezone('America/New_York')
        now_et = datetime.now(eastern)

        expiration_display = target_date.strftime('%b %d, %Y')
        if target_date == now_et.date():
            expiration_display += ' (0DTE)'

        result = {
            'timestamp': timestamp.isoformat() if timestamp else datetime.now(pytz.utc).isoformat(),
            'expiration': target_date.isoformat(),
            'expiration_display': expiration_display,
            'current_price': current_price,
            'max_pain': max_pain,
            'total_value_at_max_pain': total_value_at_max_pain,
            'strikes': strikes_list
        }

        return jsonify(result)

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/max-pain/history')
@cache_query(ttl_seconds=30)
def get_max_pain_history():
    """Get historical max pain data for time series chart - last 2 trading sessions"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET TIME ZONE 'America/New_York'")

        # Get last 48 hours of GEX metrics (covers 2 trading sessions)
        cursor.execute("""
            SELECT 
                timestamp,
                max_pain,
                underlying_price,
                expiration
            FROM gex_metrics
            WHERE symbol = 'SPY'
                AND max_pain IS NOT NULL
                AND timestamp > NOW() - INTERVAL '48 hours'
            ORDER BY timestamp ASC
        """)

        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            return jsonify({'error': 'No historical max pain data available'}), 404

        import pytz
        eastern = pytz.timezone('America/New_York')
        result = []

        for row in rows:
            ts = row['timestamp']
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            result.append({
                'timestamp': ts.isoformat(),
                'max_pain': float(row['max_pain']) if row['max_pain'] else 0,
                'underlying_price': float(row['underlying_price']) if row['underlying_price'] else 0,
                'expiration': row['expiration'].isoformat() if row['expiration'] else None
            })

        return jsonify(result)

    except Exception as e:
        print(f"Error in get_max_pain_history: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route('/api/market-status')
def get_market_status():
    """Get detailed market status based on time and recent quote freshness"""
    import pytz
    from datetime import time as dt_time

    try:
        # Get current ET time
        eastern = pytz.timezone('America/New_York')
        now_et = datetime.now(eastern)
        current_time = now_et.time()
        is_weekday = now_et.weekday() < 5  # Monday=0, Friday=4

        # Weekend or 16:00-04:00 check first
        if not is_weekday or current_time < dt_time(4, 0) or current_time >= dt_time(20, 0):
            return jsonify({
                'status': 'closed',
                'label': 'Market Closed',
                'icon': '🌙',
                'color': 'gray'
            })

        # Check for fresh data
        conn = None
        quote_is_fresh = False
        try:
            conn = get_db_connection()
            if not conn:
                return jsonify({
                    'status': 'unknown',
                    'label': 'Status Unknown (Connection Failed)',
                    'icon': '❓',
                    'color': 'gray'
                })

            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT timestamp, close
                FROM underlying_quotes
                WHERE symbol = 'SPY'
                ORDER BY timestamp DESC
                LIMIT 1
            """)

            latest_quote = cursor.fetchone()
            cursor.close()

            # Check quote freshness
            if latest_quote:
                quote_time = latest_quote['timestamp']
                if quote_time.tzinfo is None:
                    quote_time = pytz.utc.localize(quote_time)

                seconds_ago = (datetime.now(pytz.utc) - quote_time).total_seconds()
                quote_is_fresh = seconds_ago <= 30

        finally:
            if conn:
                return_db_connection(conn)

        # Streaming data, market must be open
        if quote_is_fresh:

            # Pre-market hours
            if dt_time(4, 0) <= current_time < dt_time(9, 30):
                return jsonify({
                    'status': 'pre-market',
                    'label': 'Pre-Market',
                    'icon': '🌅',
                    'color': 'amber'
                })

            # Regular market hours
            elif dt_time(9, 30) <= current_time < dt_time(16, 0):
                return jsonify({
                    'status': 'open',
                    'label': 'Market Open',
                    'icon': '☀️',
                    'color': 'green'
                })

            # After-hours
            else:
              return jsonify({
                  'status': 'after-hours',
                  'label': 'After-Hours',
                  'icon': '🌆',
                  'color': 'amber'
              })

        # Data is stale during week, assume it is a holiday
        else:
            return jsonify({
                'status': 'closed',
                'label': 'Market Closed (Holiday)',
                'icon': '🏖️',
                'color': 'gray'
            })

    except Exception as e:
        print(f"Error getting market status: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'unknown',
            'label': 'Status Unknown (Error)',
            'icon': '❓',
            'color': 'gray'
        })

@app.route('/logo_full')
def get_logo_full():
    try:
        return send_from_directory('/opt/zerogex/frontend/static', 'Dark_Full.png')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/logo_full_light')
def get_logo_full_light():
    try:
        return send_from_directory('/opt/zerogex/frontend/static', 'Light_Full.png')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/logo_title')
def get_logo_title():
    try:
        return send_from_directory('/opt/zerogex/frontend/static', 'Dark_Title.png')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/logo_title_light')
def get_logo_title_light():
    try:
        return send_from_directory('/opt/zerogex/frontend/static', 'Light_Title.png')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/logo_icon_helmet')
def get_logo_icon_helmet():
    try:
        return send_from_directory('/opt/zerogex/frontend/static', 'Dark_Helmet.png')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/logo_icon_helmet_light')
def get_logo_icon_helmet_light():
    try:
        return send_from_directory('/opt/zerogex/frontend/static', 'Light_Helmet.png')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/favicon.ico')
def get_logo_icon_ico():
    try:
        return send_from_directory('/opt/zerogex/frontend/static', 'favicon.ico')
    except Exception as e:
        print(f"Error serving logo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/navigation.js')
def serve_navigation_js():
    """Serve navigation JavaScript"""
    try:
        return send_from_directory(DASHBOARD_DIR, 'navigation.js')
    except Exception as e:
        print(f"Error serving navigation.js: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("Starting GEX Dashboard on port 8081...")
    print(f"Dashboard directory: {DASHBOARD_DIR}")
    print(f"Credentials file: {CREDS_FILE}")

    # Initialize connection pool on startup
    init_db_pool()

    app.run(host='0.0.0.0', port=8081, debug=False, threaded=True)
