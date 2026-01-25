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
    allowed_tables = ['options_quotes', 'underlying_quotes', 'gex_metrics', 'ingestion_metrics']
    if table_name not in allowed_tables:
        return jsonify({'error': 'Invalid table name'}), 400

    try:
        db_config = load_db_config()
        if not db_config:
            return jsonify({'error': 'Database config not found'}), 500

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"""
            SELECT * FROM {table_name}
            ORDER BY timestamp DESC
            LIMIT 100
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
