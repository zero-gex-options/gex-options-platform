#!/usr/bin/env python3
"""
ZeroGEX Monitoring Web Dashboard
Simple Flask app to display monitoring metrics
"""

from flask import Flask, jsonify, send_from_directory
import json
from pathlib import Path

app = Flask(__name__)
METRICS_FILE = Path("/home/ubuntu/monitoring/current_metrics.json")
DASHBOARD_DIR = Path("/opt/zerogex/monitoring")

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
