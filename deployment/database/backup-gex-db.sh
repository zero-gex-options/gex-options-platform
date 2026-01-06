#!/bin/bash

# Configuration
BACKUP_DIR="/home/ubuntu/backups"
LOG_DIR="/home/ubuntu/logs"
LOG_FILE="${LOG_DIR}/backup-gex-db.log"
DB_NAME="gex_db"
DB_USER="gex_user"
DB_HOST="localhost"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

[ ! -d "$LOG_DIR" ] && mkdir -p "$LOG_DIR"

if [ -f "$LOG_FILE" ]; then

    # Move the current log file to a timestamped backup
    mv "$LOG_FILE" "$LOG_FILE.${DATE}"

    # Compress the log file
    gzip "$LOG_FILE.${DATE}"
fi

touch "$LOG_FILE"
exec &> "${LOG_FILE}"

# Delete logs older than 7 days
DELETED=$(find "$LOG_DIR" -name "${backup-gex-db.log}.*.gz" -mtime +$RETENTION_DAYS -delete -print | wc -l)
if [ "$DELETED" -gt 0 ]; then
    echo "[$(date)] Deleted $DELETED old log(s)"
fi
 
# Create backup directory
if [ ! -d "$BACKUP_DIR" ]; then
    echo "[$(date)] Creating ${BACKUP_DIR}..."
    mkdir -p "$BACKUP_DIR"
fi

echo "[$(date)] Starting backup..."

# Backup with TimescaleDB-friendly options
# --format=custom is compressed and efficient for large databases
if pg_dump -U "$DB_USER" -h "$DB_HOST" \
    --format=custom \
    --file="$BACKUP_DIR/${DB_NAME}_${DATE}.dump" \
    "$DB_NAME" 2>&1 | grep -v "circular foreign-key" | grep -v "disable-triggers"; then
    
    echo "[$(date)] ✅ Backup completed: ${DB_NAME}_${DATE}.dump"
    
    # Get backup size
    SIZE=$(du -h "$BACKUP_DIR/${DB_NAME}_${DATE}.dump" | cut -f1)
    echo "[$(date)] Backup size: $SIZE"
    
    # Clean up old backups
    DELETED=$(find "$BACKUP_DIR" -name "${DB_NAME}_*.dump" -mtime +$RETENTION_DAYS -delete -print | wc -l)
    if [ "$DELETED" -gt 0 ]; then
        echo "[$(date)] Deleted $DELETED old backup(s)"
    fi
    
    # List current backups
    echo "[$(date)] Current backups:"
    ls -lh "$BACKUP_DIR/${DB_NAME}_"*.dump 2>/dev/null | tail -5
    
else
    echo "[$(date)] ❌ Backup failed!"
    exit 1
fi

echo "[$(date)] Backup process complete"
