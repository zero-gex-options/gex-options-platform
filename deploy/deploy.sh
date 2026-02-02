#!/bin/bash

# ==============================================
# ZeroGEX Platform Deployment Script
# ==============================================

set -e  # Exit on any error

# Export HOME
[ -z "$HOME" ] && export $HOME="/home/ubuntu"

# Variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STEPS_DIR="$SCRIPT_DIR/steps"
LOG_DIR="${HOME}/logs"
LOG_FILE="${LOG_DIR}/deployment_$(date +%Y%m%d_%H%M%S).log"

# Help text
show_help() {
    cat << EOF
ZeroGEX Platform Deployment Script

Usage: ./deploy.sh [OPTIONS]

Options:
  --start-from STEP    Start deployment from a specific step
                       STEP can be a step number (e.g., 030) or name (e.g., database)
  -h, --help          Show this help message

Examples:
  ./deploy.sh                        # Run full deployment (all steps)
  ./deploy.sh --start-from 030       # Start from step 030
  ./deploy.sh --start-from database  # Start from database step
  ./deploy.sh --start-from monitoring # Start from monitoring step

Available Steps:
  010.setup          - System setup and configuration
  015.data_volume    - Data volume setup (/data mount and structure)
  020.database       - PostgreSQL + TimescaleDB setup (uses /data/postgresql)
  030.application    - Application setup and dependencies
  040.tokens         - TradeStation token initialization
  050.security       - Security hardening (firewall, SSH)
  060.backups        - Automated database backups (uses /data/backups)
  070.systemd        - Systemd services configuration
  072.gex_cli_tools  - GEX CLI tools installation
  080.validation     - Deployment validation
  090.monitoring     - Monitoring system setup (uses /data/monitoring)

Deployment Flow:
  1. System packages and timezone setup
  2. Mount /data volume and create directory structure
  3. Install and configure PostgreSQL on /data
  4. Setup Python application and dependencies
  5. Initialize TradeStation API tokens
  6. Harden security (SSH, firewall, auto-updates)
  7. Configure automated database backups
  8. Setup and start systemd services
  9. Install GEX CLI tools
  10. Validate deployment
  11. Setup monitoring dashboard

Data Storage:
  /data/postgresql   - PostgreSQL database files
  /data/backups      - Database backup files
  /data/monitoring   - Monitoring metrics (JSON)

Logs are saved to: /home/ubuntu/logs/deployment_YYYYMMDD_HHMMSS.log

EOF
    exit 0
}

# Parse command line arguments
START_FROM=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --start-from)
            START_FROM="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Create logs directory if it doesn't exist
[ ! -d "$LOG_DIR" ] && mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Export log function for sub-steps
export -f log
export LOG_FILE

log "=========================================="
log "🚀 Deploying GEX Options Platform..."
if [ -n "$START_FROM" ]; then
    log "Starting from step: $START_FROM"
fi
log "=========================================="

# Flag to track if we should start executing
SHOULD_EXECUTE=false
if [ -z "$START_FROM" ]; then
    SHOULD_EXECUTE=true
fi

# Execute each step in order
for step_script in "$STEPS_DIR"/*.* ; do
    if [ -x "$step_script" ]; then
        step_name=$(basename "$step_script")

        # Check if this is the start-from step
        if [ -n "$START_FROM" ] && [[ "$step_name" == *"$START_FROM"* ]]; then
            SHOULD_EXECUTE=true
            log "Found start step: $step_name"
        fi

        # Skip steps before the start-from step
        if [ "$SHOULD_EXECUTE" = false ]; then
            log "Skipping: $step_name"
            continue
        fi

	log "=========================================="
        log "Executing: $step_name ..."

        if bash "$step_script"; then
            log "✓ $step_name completed successfully"
        else
            log "✗ $step_name failed"
            exit 1
        fi
        log ""
    fi
done

log ""
log "=========================================="
log "✅ Deployment Complete!"
log "=========================================="
log "Log file: $LOG_FILE"
