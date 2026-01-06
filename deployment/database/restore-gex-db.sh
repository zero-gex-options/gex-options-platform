#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <backup_file>"
    echo ""
    echo "Available backups:"
    ls -lh /home/ubuntu/backups/gex_db_*.{dump,sql.gz} 2>/dev/null | tail -10
    exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "❌ Backup file not found: $BACKUP_FILE"
    exit 1
fi

echo "⚠️  WARNING: This will REPLACE the current database!"
echo "Backup file: $BACKUP_FILE"
echo "File size: $(du -h "$BACKUP_FILE" | cut -f1)"
echo ""
read -p "Are you sure? (type 'yes' to continue): " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Restore cancelled"
    exit 0
fi

echo "[$(date)] Stopping services..."
sudo systemctl stop gex-ingestion
sudo systemctl stop gex-scheduler

echo "[$(date)] Dropping existing database..."
psql -U postgres -h localhost << EOF
DROP DATABASE IF EXISTS gex_db;
CREATE DATABASE gex_db;
GRANT ALL PRIVILEGES ON DATABASE gex_db TO gex_user;
EOF

echo "[$(date)] Restoring database..."

# Check file extension and restore accordingly
if [[ "$BACKUP_FILE" == *.dump ]]; then
    # Custom format backup (preferred)
    pg_restore -U gex_user -h localhost -d gex_db "$BACKUP_FILE" 2>&1 | \
        grep -v "circular foreign-key" | grep -v "disable-triggers"
    RESTORE_STATUS=$?
elif [[ "$BACKUP_FILE" == *.sql.gz ]]; then
    # Gzipped SQL backup (legacy)
    gunzip -c "$BACKUP_FILE" | psql -U gex_user -h localhost gex_db 2>&1 | \
        grep -v "circular foreign-key" | grep -v "disable-triggers"
    RESTORE_STATUS=$?
else
    echo "❌ Unknown backup format (expected .dump or .sql.gz)"
    exit 1
fi

if [ $RESTORE_STATUS -eq 0 ]; then
    echo "[$(date)] ✅ Database restored successfully"
else
    echo "[$(date)] ❌ Restore failed with status $RESTORE_STATUS"
    exit 1
fi

# Ensure permissions are correct
echo "[$(date)] Setting permissions..."
psql -U postgres -d gex_db -h localhost << EOF
GRANT ALL ON SCHEMA public TO gex_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO gex_user;
EOF

echo "[$(date)] Starting services..."
sudo systemctl start gex-ingestion
sudo systemctl start gex-scheduler

echo "[$(date)] ✅ Restore complete!"

# Verify data
echo "[$(date)] Verifying data..."
psql -U gex_user -d gex_db -h localhost << EOF
SELECT 'Options quotes:', COUNT(*) FROM options_quotes;
SELECT 'GEX metrics:', COUNT(*) FROM gex_metrics;
SELECT 'Latest data:', MAX(timestamp) FROM options_quotes;
EOF
