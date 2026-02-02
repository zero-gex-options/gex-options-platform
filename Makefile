# ZeroGEX Platform Makefile
# Convenient commands for development and deployment

.PHONY: help deploy deploy-gex migrate-gex test clean install

help:
	@echo "ZeroGEX Platform - Available Commands"
	@echo "======================================"
	@echo ""
	@echo "Deployment:"
	@echo "  make deploy           - Full platform deployment (all steps)"
	@echo "  make deploy-from-020  - Deploy from database step (020.database)"
	@echo "  make deploy-from-070  - Deploy from systemd step (updates services)"
	@echo "  make migrate-gex      - Quick GEX module migration"
	@echo ""
	@echo "Deployment Steps (in order):"
	@echo "  010.setup          - System setup and configuration"
	@echo "  015.data_volume    - Data volume setup (/data mount and structure)"
	@echo "  020.database       - PostgreSQL + TimescaleDB (uses /data/postgresql)"
	@echo "  030.application    - Application setup and dependencies"
	@echo "  040.tokens         - TradeStation token initialization"
	@echo "  050.security       - Security hardening (firewall, SSH)"
	@echo "  060.backups        - Automated backups (uses /data/backups)"
	@echo "  070.systemd        - Systemd services configuration"
	@echo "  072.gex_cli_tools  - GEX CLI tools installation"
	@echo "  080.validation     - Deployment validation"
	@echo "  090.monitoring     - Monitoring setup (uses /data/monitoring)"
	@echo ""
	@echo "Development:"
	@echo "  make install          - Install Python dependencies"
	@echo "  make test             - Run tests"
	@echo "  make clean            - Clean Python cache files"
	@echo "  make venv             - Create virtual environment"
	@echo "  make format           - Format code with black"
	@echo "  make lint             - Lint code with pylint"
	@echo ""
	@echo "GEX Tools:"
	@echo "  make gex-cli         - Install GEX CLI tools"
	@echo "  make gex-calc        - Quick GEX calculation"
	@echo "  make gex-summary     - GEX summary"
	@echo ""
	@echo "Services:"
	@echo "  make services-status - Check all service statuses"
	@echo "  make services-logs   - View all service logs"
	@echo "  make restart-gex     - Restart GEX scheduler"
	@echo "  make restart-all     - Restart all services"
	@echo ""
	@echo "Database:"
	@echo "  make db-shell        - Open database shell"
	@echo "  make db-status       - Check database status"
	@echo "  make db-backup       - Manual database backup"
	@echo ""
	@echo "Monitoring:"
	@echo "  make health          - Platform health check"
	@echo "  make logs-ingestion  - View ingestion logs"
	@echo "  make logs-scheduler  - View scheduler logs"
	@echo "  make logs-monitor    - View monitor logs"
	@echo ""
	@echo "Data Storage Locations:"
	@echo "  /data/postgresql   - PostgreSQL database files"
	@echo "  /data/backups      - Database backup files"
	@echo "  /data/monitoring   - Monitoring metrics (JSON)"
	@echo ""

# Deployment
deploy:
	@echo "Running full deployment..."
	./deploy/deploy.sh

deploy-from-020:
	@echo "Deploying from database step (020)..."
	./deploy/deploy.sh --start-from 020

deploy-from-070:
	@echo "Deploying from systemd step (070)..."
	./deploy/deploy.sh --start-from 070

migrate-gex:
	@echo "Running GEX module migration..."
	chmod +x scripts/migrate_gex_module.sh
	./scripts/migrate_gex_module.sh

# Development
install:
	@echo "Installing Python dependencies..."
	source venv/bin/activate && pip install -r requirements.txt

test:
	@echo "Running tests..."
	source venv/bin/activate && python -m pytest tests/

clean:
	@echo "Cleaning Python cache files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# GEX Tools
gex-cli:
	@echo "Installing GEX CLI tools..."
	sudo ./deploy/steps/072.gex_cli_tools

gex-calc:
	@echo "Calculating GEX for SPY..."
	source venv/bin/activate && python src/gex/gex_calculator.py

gex-summary:
	@gex-cli summary SPY || (echo "Run 'make gex-cli' first to install CLI tools" && exit 1)

# Services
services-status:
	@echo "Service Status:"
	@echo "==============="
	@systemctl is-active gex-ingestion && echo "✓ gex-ingestion: RUNNING" || echo "✗ gex-ingestion: STOPPED"
	@systemctl is-active gex-scheduler && echo "✓ gex-scheduler: RUNNING" || echo "✗ gex-scheduler: STOPPED"
	@systemctl is-active postgresql && echo "✓ postgresql: RUNNING" || echo "✗ postgresql: STOPPED"
	@systemctl is-active gex-monitor && echo "✓ gex-monitor: RUNNING" || echo "✗ gex-monitor: STOPPED"
	@systemctl is-active gex-dashboard && echo "✓ gex-dashboard: RUNNING" || echo "✗ gex-dashboard: STOPPED"

services-logs:
	@echo "Opening service logs (Ctrl+C to exit)..."
	sudo journalctl -u gex-ingestion -u gex-scheduler -u gex-monitor -f

restart-gex:
	@echo "Restarting GEX scheduler..."
	sudo systemctl restart gex-scheduler
	@echo "✓ GEX scheduler restarted"

restart-all:
	@echo "Restarting all services..."
	sudo systemctl restart gex-ingestion
	sudo systemctl restart gex-scheduler
	sudo systemctl restart gex-monitor
	sudo systemctl restart gex-dashboard
	@echo "✓ All services restarted"

# Database
db-shell:
	@psql -U gex_user -d gex_db -h localhost

db-status:
	@echo "Database Status:"
	@echo "================"
	@psql -U gex_user -d gex_db -h localhost << 'SQL'
	SELECT 
	    'Options Quotes' as table_name,
	    COUNT(*) as total_records,
	    MAX(timestamp) as latest_record
	FROM options_quotes
	UNION ALL
	SELECT 
	    'GEX Metrics',
	    COUNT(*),
	    MAX(timestamp)
	FROM gex_metrics
	UNION ALL
	SELECT 
	    'Underlying Quotes',
	    COUNT(*),
	    MAX(timestamp)
	FROM underlying_quotes;
	SQL

db-backup:
	@echo "Running manual database backup..."
	@sudo /usr/local/bin/backup-gex-db.sh
	@echo "✓ Backup complete"
	@echo "Location: /data/backups/"

# Development helpers
venv:
	@echo "Creating virtual environment..."
	python3 -m venv venv
	@echo "Activating and installing dependencies..."
	source venv/bin/activate && pip install -r requirements.txt
	@echo "✓ Virtual environment ready"

format:
	@echo "Formatting Python code with black..."
	source venv/bin/activate && black src/ tests/ || echo "Install black: pip install black"

lint:
	@echo "Linting Python code..."
	source venv/bin/activate && pylint src/ || echo "Install pylint: pip install pylint"

# Quick checks
health:
	@echo "Platform Health Check"
	@echo "====================="
	@make services-status
	@echo ""
	@make db-status
	@echo ""
	@echo "Data Storage:"
	@echo "  PostgreSQL: $$(du -sh /data/postgresql 2>/dev/null | cut -f1 || echo 'N/A')"
	@echo "  Backups:    $$(du -sh /data/backups 2>/dev/null | cut -f1 || echo 'N/A')"
	@echo "  Monitoring: $$(du -sh /data/monitoring 2>/dev/null | cut -f1 || echo 'N/A')"

logs-ingestion:
	sudo journalctl -u gex-ingestion -f

logs-scheduler:
	sudo journalctl -u gex-scheduler -f

logs-monitor:
	sudo journalctl -u gex-monitor -f

logs-dashboard:
	sudo journalctl -u gex-dashboard -f
