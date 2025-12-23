#!/bin/bash

# Configuration
BACKUP_DIR="/home/ubuntu/backups"
DB_NAME="gex_db"
DB_USER="gex_user"
DB_HOST="localhost"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

# Create backup directory
mkdir -p "$BACKUP_DIR"

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
