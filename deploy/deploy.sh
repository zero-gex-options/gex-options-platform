#!/bin/bash

# ==============================================
# ZeroGEX Platform Deployment Script
# ==============================================

set -e  # Exit on any error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STEPS_DIR="$SCRIPT_DIR/steps"
LOG_FILE="/home/ubuntu/logs/deployment_$(date +%Y%m%d_%H%M%S).log"

# Create logs directory if it doesn't exist
LOG_DIR="/home/ubuntu/logs"
[ ! -d "$LOG_DIR" ] && mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "🚀 Deploying GEX Options Platform..."
log "=========================================="
log ""

# Execute each step in order
for step_script in "$STEPS_DIR"/*.* ; do
    if [ -x "$step_script" ]; then
        step_name=$(basename "$step_script")
        log "Executing: $step_name"

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
