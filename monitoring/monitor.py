#!/usr/bin/env python3
"""
ZeroGEX Platform Monitoring System
Collects metrics and provides real-time dashboard
"""

import os
import sys
import time
import json
import psutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
import threading
from collections import deque

# Try to import optional dependencies
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

class MonitoringCollector:
    """Collects system and application metrics"""

    def __init__(self, db_config: Optional[Dict] = None):
        self.db_config = db_config
        self.metrics_history = deque(maxlen=1440)  # 24 hours at 1-min intervals
        self.alerts = []

    def get_system_metrics(self) -> Dict:
        """Collect system-level metrics"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')

        return {
            'timestamp': datetime.now().isoformat(),
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'memory_used_gb': memory.used / (1024**3),
            'memory_total_gb': memory.total / (1024**3),
            'disk_percent': disk.percent,
            'disk_used_gb': disk.used / (1024**3),
            'disk_total_gb': disk.total / (1024**3),
        }

    def get_service_status(self) -> Dict:
        """Check systemd service status"""
        services = ['gex-ingestion', 'gex-scheduler', 'postgresql', 'fail2ban']
        status = {}

        for service in services:
            try:
                result = subprocess.run(
                    ['systemctl', 'is-active', service],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                status[service] = result.stdout.strip()
            except Exception as e:
                status[service] = f'error: {str(e)}'

        return status

    def get_database_metrics(self) -> Optional[Dict]:
        """Collect database metrics"""
        if not HAS_PSYCOPG2 or not self.db_config:
            return None

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Get recent data counts from options_quotes
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '10 minutes') as recent_10min,
                    COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') as recent_1hour,
                    MAX(timestamp) as latest_timestamp
                FROM options_quotes;
            """)
            quotes_data = cursor.fetchone()

            # Get GEX calculations
            cursor.execute("""
                SELECT COUNT(*) as gex_count, MAX(timestamp) as latest_gex
                FROM gex_metrics;
            """)
            gex_data = cursor.fetchone()

            # Database size
            cursor.execute("""
                SELECT pg_size_pretty(pg_database_size('gex_db')) as db_size;
            """)
            size_data = cursor.fetchone()

            # Active connections
            cursor.execute("""
                SELECT COUNT(*) as active_connections
                FROM pg_stat_activity
                WHERE datname = 'gex_db';
            """)
            conn_data = cursor.fetchone()

            # Get most recent underlying quote
            cursor.execute("""
                SELECT symbol, close as price, total_volume as volume, timestamp
                FROM underlying_quotes
                ORDER BY timestamp DESC
                LIMIT 1;
            """)
            underlying_quote = cursor.fetchone()

            # Get 50 most recent option quotes
            cursor.execute("""
                SELECT symbol, strike, expiration, dte, option_type, last, 
                       bid, ask, mid, volume, open_interest, implied_vol, 
                       delta, gamma, theta, vega, timestamp
                FROM options_quotes
                ORDER BY timestamp DESC
                LIMIT 50;
            """)
            recent_options = cursor.fetchall()

            # Get underlying price history (all available data)
            cursor.execute("""
                SELECT timestamp, symbol, close as price, total_volume as volume
                FROM underlying_quotes
                ORDER BY timestamp ASC;
            """)
            underlying_history_raw = cursor.fetchall()
            underlying_history = [dict(row) for row in underlying_history_raw] if underlying_history_raw else []

            # Get ingestion metrics history (last 48 hours)
            cursor.execute("""
                SELECT timestamp, records_ingested, error_count
                FROM ingestion_metrics
                WHERE timestamp > NOW() - INTERVAL '48 hours'
                ORDER BY timestamp ASC;
            """)
            ingestion_history_raw = cursor.fetchall()
            ingestion_history = [dict(row) for row in ingestion_history_raw] if ingestion_history_raw else []

            # Get most recent ingestion metrics
            cursor.execute("""
                SELECT
                    MAX(timestamp) as timestamp,
                    source,
                    symbol,
                    SUM(records_ingested) as records_ingested,
                    SUM(error_count) as error_count,
                    AVG(processing_time_ms)::BIGINT as processing_time_ms,
                    SUM(heartbeat_count) as heartbeat_count,
                    MAX(last_heartbeat) as last_heartbeat
                FROM ingestion_metrics
                WHERE timestamp > NOW() - INTERVAL '1 hour'
                GROUP BY source, symbol
                ORDER BY MAX(timestamp) DESC
                LIMIT 1;
            """)
            ingestion_metric = cursor.fetchone()

            cursor.close()
            conn.close()

            return {
                'quotes_total': quotes_data['total_rows'] if quotes_data else 0,
                'quotes_recent_10min': quotes_data['recent_10min'] if quotes_data else 0,
                'quotes_recent_1hour': quotes_data['recent_1hour'] if quotes_data else 0,
                'latest_quote': quotes_data['latest_timestamp'].isoformat() if quotes_data and quotes_data['latest_timestamp'] else None,
                'gex_count': gex_data['gex_count'] if gex_data else 0,
                'latest_gex': gex_data['latest_gex'].isoformat() if gex_data and gex_data['latest_gex'] else None,
                'db_size': size_data['db_size'] if size_data else 'unknown',
                'active_connections': conn_data['active_connections'] if conn_data else 0,
                'spy_quote': dict(underlying_quote) if underlying_quote else None,  # ADD THIS LINE
                'underlying_history': underlying_history,
                'recent_options': [dict(row) for row in recent_options] if recent_options else [],
                'ingestion_metric': dict(ingestion_metric) if ingestion_metric else None,
                'ingestion_history': ingestion_history,
            }
        except Exception as e:
            return {'error': str(e)}

    def get_log_errors(self, service: str, minutes: int = 10) -> List[str]:
        """Get recent errors from service logs"""
        try:
            result = subprocess.run(
                ['journalctl', '-u', service, '--since', f'{minutes} minutes ago', '-p', 'err'],
                capture_output=True,
                text=True,
                timeout=10
            )
            lines = result.stdout.strip().split('\n')
            return [line for line in lines if line][-10:]  # Last 10 errors
        except Exception as e:
            return [f'Error reading logs: {str(e)}']

    def check_alerts(self, metrics: Dict) -> List[Dict]:
        """Check for alert conditions"""
        alerts = []
        timestamp = datetime.now().isoformat()

        # Check if market is open (9:30 AM - 4:00 PM ET, Mon-Fri)
        now = datetime.now()
        is_market_open = False
        if now.weekday() < 5:  # Monday = 0, Friday = 4
            market_open = now.replace(hour=9, minute=30, second=0)
            market_close = now.replace(hour=16, minute=0, second=0)
            is_market_open = market_open <= now <= market_close

        # Store market status in metrics
        metrics['market_open'] = is_market_open

        # CPU alert
        if metrics['system']['cpu_percent'] > 90:
            alerts.append({
                'timestamp': timestamp,
                'level': 'critical',
                'category': 'system',
                'message': f"High CPU usage: {metrics['system']['cpu_percent']:.1f}%"
            })
        elif metrics['system']['cpu_percent'] > 80:
            alerts.append({
                'timestamp': timestamp,
                'level': 'warning',
                'category': 'system',
                'message': f"Elevated CPU usage: {metrics['system']['cpu_percent']:.1f}%"
            })

        # Memory alert
        if metrics['system']['memory_percent'] > 90:
            alerts.append({
                'timestamp': timestamp,
                'level': 'critical',
                'category': 'system',
                'message': f"High memory usage: {metrics['system']['memory_percent']:.1f}%"
            })
        elif metrics['system']['memory_percent'] > 80:
            alerts.append({
                'timestamp': timestamp,
                'level': 'warning',
                'category': 'system',
                'message': f"Elevated memory usage: {metrics['system']['memory_percent']:.1f}%"
            })

        # Disk alert
        if metrics['system']['disk_percent'] > 90:
            alerts.append({
                'timestamp': timestamp,
                'level': 'critical',
                'category': 'system',
                'message': f"High disk usage: {metrics['system']['disk_percent']:.1f}%"
            })
        elif metrics['system']['disk_percent'] > 85:
            alerts.append({
                'timestamp': timestamp,
                'level': 'warning',
                'category': 'system',
                'message': f"Elevated disk usage: {metrics['system']['disk_percent']:.1f}%"
            })

        # Service alerts
        for service, status in metrics['services'].items():
            if status != 'active':
                alerts.append({
                    'timestamp': timestamp,
                    'level': 'critical',
                    'category': 'service',
                    'message': f"Service {service} is {status}"
                })

        # Database alerts
        if metrics.get('database') and not metrics['database'].get('error'):
            db = metrics['database']

            # No recent data - only alert during market hours
            if db.get('quotes_recent_10min', 0) == 0 and is_market_open:
                alerts.append({
                    'timestamp': timestamp,
                    'level': 'warning',
                    'category': 'data',
                    'message': 'No data ingested in last 10 minutes (during market hours)'
                })

        return alerts

    def collect_all_metrics(self) -> Dict:
        """Collect all metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'system': self.get_system_metrics(),
            'services': self.get_service_status(),
            'database': self.get_database_metrics(),
        }

        # Add service error logs
        metrics['errors'] = {
            'ingestion': self.get_log_errors('gex-ingestion', 10),
            'scheduler': self.get_log_errors('gex-scheduler', 10),
        }

        # Check for alerts
        alerts = self.check_alerts(metrics)
        metrics['alerts'] = alerts

        # Calculate uptime percentage for current hour
        metrics['uptime_current_hour'] = self.calculate_uptime_current_hour()

        # Store in history
        self.metrics_history.append(metrics)

        return metrics

    def track_service_uptime(self):
        """Track service uptime status every check"""
        if not HAS_PSYCOPG2 or not self.db_config:
            return

        try:
            # Create temporary database connection
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Check if gex-ingestion is running
            result = subprocess.run(
                ['systemctl', 'is-active', 'gex-ingestion'],
                capture_output=True,
                text=True,
                timeout=5
            )

            is_up = 1 if result.stdout.strip() == 'active' else 0

            insert_query = """
                INSERT INTO service_uptime_checks
                (timestamp, service_name, is_up)
                VALUES (%s, %s, %s)
            """

            cursor.execute(insert_query, (
                datetime.now(timezone.utc),
                'gex-ingestion',
                is_up
            ))

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            print(f"Failed to track service uptime: {e}")

    def calculate_uptime_current_hour(self) -> Dict:
        """Calculate uptime percentage for the current hour"""
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        minutes_elapsed = (now - hour_start).seconds / 60

        uptime_pct = (minutes_elapsed / 60) * 100 if minutes_elapsed > 0 else 0

        return {
            'hour_label': hour_start.strftime('%m/%d %H:00'),
            'uptime_percent': min(uptime_pct, 100),
            'minutes_up': minutes_elapsed
        }


class MonitoringDashboard:
    """Terminal-based monitoring dashboard"""

    def __init__(self, collector: MonitoringCollector):
        self.collector = collector
        self.running = False

    def clear_screen(self):
        """Clear terminal screen"""
        os.system('clear' if os.name == 'posix' else 'cls')

    def format_uptime(self, seconds: float) -> str:
        """Format uptime as human-readable string"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{days}d {hours}h {minutes}m"

    def render_dashboard(self, metrics: Dict):
        """Render the monitoring dashboard"""
        self.clear_screen()

        # Header
        print("=" * 80)
        print(f"{'ZeroGEX Platform Monitor':^80}")
        print(f"{'Last Updated: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^80}")
        print("=" * 80)
        print()

        # System Metrics
        sys_metrics = metrics['system']
        print("┌─ SYSTEM RESOURCES ─────────────────────────────────────────────────────────┐")
        print(f"│ CPU Usage:    {self._format_bar(sys_metrics['cpu_percent'], 100)} {sys_metrics['cpu_percent']:>5.1f}%  │")
        print(f"│ Memory:       {self._format_bar(sys_metrics['memory_percent'], 100)} {sys_metrics['memory_percent']:>5.1f}%  │")
        print(f"│               {sys_metrics['memory_used_gb']:.1f}GB / {sys_metrics['memory_total_gb']:.1f}GB{' ' * 38}│")
        print(f"│ Disk:         {self._format_bar(sys_metrics['disk_percent'], 100)} {sys_metrics['disk_percent']:>5.1f}%  │")
        print(f"│               {sys_metrics['disk_used_gb']:.1f}GB / {sys_metrics['disk_total_gb']:.1f}GB{' ' * 38}│")
        print("└────────────────────────────────────────────────────────────────────────────┘")
        print()

        # Services
        print("┌─ SERVICE STATUS ───────────────────────────────────────────────────────────┐")
        for service, status in metrics['services'].items():
            status_icon = "●" if status == "active" else "○"
            status_color = "\033[92m" if status == "active" else "\033[91m"
            print(f"│ {status_color}{status_icon}\033[0m {service:<30} {status:<40}│")
        print("└────────────────────────────────────────────────────────────────────────────┘")
        print()

        # Database Metrics
        if metrics.get('database') and not metrics['database'].get('error'):
            db = metrics['database']
            print("┌─ DATABASE METRICS ─────────────────────────────────────────────────────────┐")
            print(f"│ Total Quotes:        {db.get('quotes_total', 0):>10,}                                       │")
            print(f"│ Recent (10 min):     {db.get('quotes_recent_10min', 0):>10,}                                       │")
            print(f"│ Recent (1 hour):     {db.get('quotes_recent_1hour', 0):>10,}                                       │")
            print(f"│ GEX Calculations:    {db.get('gex_count', 0):>10,}                                       │")
            print(f"│ Database Size:       {db.get('db_size', 'unknown'):<63}│")
            print(f"│ Active Connections:  {db.get('active_connections', 0):>10}                                       │")

            latest = db.get('latest_quote')
            if latest:
                age = (datetime.now() - datetime.fromisoformat(latest)).total_seconds()
                print(f"│ Latest Data:         {age:.0f}s ago                                            │")
            print("└────────────────────────────────────────────────────────────────────────────┘")
            print()

        # Alerts
        alerts = metrics.get('alerts', [])
        if alerts:
            print("┌─ ALERTS ───────────────────────────────────────────────────────────────────┐")
            for alert in alerts[-5:]:  # Show last 5 alerts
                level_color = "\033[91m" if alert['level'] == 'critical' else "\033[93m"
                level_icon = "⚠" if alert['level'] == 'warning' else "✗"
                msg = alert['message'][:65]
                print(f"│ {level_color}{level_icon}\033[0m {msg:<72}│")
            print("└────────────────────────────────────────────────────────────────────────────┘")
            print()

        # Footer
        print("Press Ctrl+C to exit")

    def _format_bar(self, value: float, max_value: float, width: int = 40) -> str:
        """Format a progress bar"""
        filled = int((value / max_value) * width)
        bar = "█" * filled + "░" * (width - filled)

        # Color based on percentage
        if value >= 90:
            color = "\033[91m"  # Red
        elif value >= 80:
            color = "\033[93m"  # Yellow
        else:
            color = "\033[92m"  # Green

        return f"{color}{bar}\033[0m"

    def run(self, interval: int = 5):
        """Run the dashboard with auto-refresh"""
        self.running = True
        try:
            while self.running:
                metrics = self.collector.collect_all_metrics()
                self.render_dashboard(metrics)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped by user.")
            self.running = False


class MetricsExporter:
    """Export metrics to JSON files for external consumption"""

    def __init__(self, output_dir: str = "/data/monitoring"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_metrics(self, metrics: Dict):
        """Export current metrics to JSON"""
        current_file = self.output_dir / "current_metrics.json"
        with open(current_file, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)

        # Also append to daily log
        date_str = datetime.now().strftime('%Y%m%d')
        daily_file = self.output_dir / f"metrics_{date_str}.jsonl"
        with open(daily_file, 'a') as f:
            f.write(json.dumps(metrics, default=str) + '\n')


def load_db_config() -> Optional[Dict]:
    """Load database configuration from ~/.zerogex_db_creds file"""
    creds_file = Path.home() / ".zerogex_db_creds"
    if not creds_file.exists():
        return None

    config = {}
    with open(creds_file) as f:
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


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='ZeroGEX Platform Monitor')
    parser.add_argument('--interval', type=int, default=5, help='Refresh interval in seconds')
    parser.add_argument('--export', action='store_true', help='Enable metrics export to JSON')
    parser.add_argument('--export-dir', default='/home/ubuntu/monitoring', help='Export directory')
    parser.add_argument('--daemon', action='store_true', help='Run as background daemon')

    args = parser.parse_args()

    # Load database config from ~/.zerogex_db_creds
    db_config = load_db_config()
    if not db_config:
        print("Warning: Could not load database configuration from ~/.zerogex_db_creds")
        print("Database metrics will not be available.")
        db_config = None

    # Initialize collector
    collector = MonitoringCollector(db_config)

    # Initialize exporter if requested
    exporter = MetricsExporter(args.export_dir) if args.export else None

    if args.daemon:
        print(f"Running monitoring daemon (interval: {args.interval}s)")
        print(f"Metrics export: {'enabled' if args.export else 'disabled'}")
        if args.export:
            print(f"Export directory: {args.export_dir}")
        print("Press Ctrl+C to stop")
        print()

        try:
            while True:
                metrics = collector.collect_all_metrics()

                # Track service uptime
                collector.track_service_uptime()

                if exporter:
                    exporter.export_metrics(metrics)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nDaemon stopped by user.")
    else:
        # Run interactive dashboard
        dashboard = MonitoringDashboard(collector)
        dashboard.run(interval=args.interval)


if __name__ == '__main__':
    main()
