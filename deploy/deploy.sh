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
  020.database       - PostgreSQL + TimescaleDB setup
  030.application    - Application setup and dependencies
  040.systemd        - Systemd services configuration
  050.security       - Security hardening
  060.backups        - Automated database backups
  070.tradestation   - TradeStation token initialization
  080.validation     - Deployment validation
  090.monitoring     - Monitoring system setup

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
