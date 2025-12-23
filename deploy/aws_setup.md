##########################################################################################
### Step 1: Launch EC2 Instance

1. **Go to AWS Console → EC2 → Launch Instance**

2. **Choose Configuration:**
   - **Name:** `gex-options-platform`
   - **AMI:** Ubuntu Server 22.04 LTS
   - **Instance Type:** `t3.medium` (2 vCPU, 4GB RAM minimum)
     - For production with high volume: `t3.large` or `t3.xlarge`
   - **Key Pair:** Create new or use existing
   - **Storage:** 50 GB gp3 SSD minimum

3. **Security Group Rules:**
```
   SSH (22)        - Your IP only
   PostgreSQL (5432) - Localhost only (for now)
   HTTP (80)       - Anywhere (for future dashboard)
   HTTPS (443)     - Anywhere (for future dashboard)

##########################################################################################
### Step 2: Connect and Initial Setup

1. Connect to your instance
ssh -i your-key.pem ubuntu@YOUR_EC2_IP

2. Update system
sudo apt update && sudo apt upgrade -y

3. Install essential tools
sudo apt install -y git curl wget build-essential python3-pip python3-venv postgresql postgresql-contrib

4. Update timezone
sudo timedatectl set-timezone America/New_York

5. Verify timezone update
date

##########################################################################################
### Step 3: Install TimescaleDB

1. Add TimescaleDB repository
sudo apt install -y gnupg postgresql-common apt-transport-https lsb-release wget

2. Add TimescaleDB PGP key
sudo sh -c "echo "deb [signed-by=/usr/share/keyrings/timescaledb-archive-keyring.gpg] https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list"
wget -qO- https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/timescaledb-archive-keyring.gpg

3. Update and install
sudo apt update
sudo apt install -y timescaledb-2-postgresql-14
# If Unable to locate package timescaledb-2-postgresql-14, try:
PG_VERSION=$(psql --version | grep -oP '\d+' | head -1)
echo "PostgreSQL version: $PG_VERSION"
sudo apt install -y gnupg postgresql-common apt-transport-https lsb-release wget
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/timescaledb.gpg > /dev/null
sudo apt update
sudo apt install -y timescaledb-2-postgresql-${PG_VERSION}

4. Tune PostgreSQL for TimescaleDB
sudo timescaledb-tune --quiet --yes

5. Restart PostgreSQL
sudo systemctl restart postgresql

##########################################################################################
### Step 4: Configure PostgreSQL

1. Switch to postgres user
sudo -i -u postgres

2. Create database and user
***** Update your_secure_password_here ****
psql << EOF
CREATE DATABASE gex_db;
CREATE USER gex_user WITH PASSWORD 'your_secure_password_here';
GRANT ALL PRIVILEGES ON DATABASE gex_db TO gex_user;
\c gex_db
GRANT ALL ON SCHEMA public TO gex_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO gex_user;
EOF

3. Exit postgres user
exit

4. Configure PostgreSQL to allow connections
sudo vi /etc/postgresql/14/main/postgresql.conf

5. Edit postgresql.conf
# Find and change:
listen_addresses = 'localhost'  # Keep localhost only for security

6. Edit pg_hba.conf
sudo vi /etc/postgresql/14/main/pg_hba.conf
# Add this line:
# Allow local connections
local   all             gex_user                                md5
host    all             gex_user        127.0.0.1/32            md5

7. Restart PostgreSQL
sudo systemctl restart postgresql

8. Test connection
psql -U gex_user -d gex_db -h localhost
# Enter password when prompted
# Type \q to quit

##########################################################################################
### Step 5: Deploy Application Code

1.a. Setup SSH keypair (if already done, skip to 1.b.)
ssh-keygen -t ed25519 -C "zerogexoptions@gmail.com"
chmod 0600 .ssh/id_ed25519.pub
cat .ssh/id_ed25519.pub
# Add new SSH key in GitHub and copy/paste public key

1.b. Clone your repository
cd /home/ubuntu
git clone git@github.com:zero-gex-options/gex-options-platform.git
cd gex-options-platform

2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

4. Create .env file
vi .env
# Update with secret credentials and customizations

5. Set proper permissions
chmod 600 .env

pip install --upgrade pip

6. Initialize database schema
psql -U gex_user -d gex_db -h localhost -f config/database_schema.sql

##########################################################################################
### Step 6: Create Systemd Services

1. Ingestion Service
sudo vi /etc/systemd/system/gex-ingestion.service
# Add:
[Unit]
Description=GEX Options Data Ingestion
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/gex-options-platform/src/ingestion
Environment="PATH=/home/ubuntu/gex-options-platform/venv/bin"
Environment="PYTHONPATH=/home/ubuntu/gex-options-platform/src/ingestion"
EnvironmentFile=/home/ubuntu/gex-options-platform/.env

ExecStart=/home/ubuntu/gex-options-platform/venv/bin/python tradestation_streaming_ingestion_engine.py

# Restart behavior
Restart=always
RestartSec=10

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=gex-ingestion

[Install]
WantedBy=multi-user.target

2. GEX Scheduler Service:
sudo vi /etc/systemd/system/gex-scheduler.service
# Add:
[Unit]
Description=GEX Calculation Scheduler
After=network.target postgresql.service gex-ingestion.service
Wants=postgresql.service

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/gex-options-platform/src/gex
Environment="PATH=/home/ubuntu/gex-options-platform/venv/bin"
Environment="PYTHONPATH=/home/ubuntu/gex-options-platform/src/gex"
EnvironmentFile=/home/ubuntu/gex-options-platform/.env

ExecStart=/home/ubuntu/gex-options-platform/venv/bin/python gex_scheduler.py

# Restart behavior
Restart=always
RestartSec=10

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=gex-scheduler

[Install]
WantedBy=multi-user.target

3. Reload systemd
sudo systemctl daemon-reload

4. Enable services to start on boot
sudo systemctl enable gex-ingestion
sudo systemctl enable gex-scheduler

5. Start services
sudo systemctl start gex-ingestion
sudo systemctl start gex-scheduler

6. Check status
sudo systemctl status gex-ingestion
sudo systemctl status gex-scheduler

##########################################################################################
### Step 7: Monitoring and Logs

1. View Ingestion logs
sudo journalctl -u gex-ingestion -f

2. View Scheduler logs
sudo journalctl -u gex-scheduler -f

3. View both services
sudo journalctl -u gex-ingestion -u gex-scheduler -f

4. View PostgreSQL logs
sudo journalctl -u postgresql -f

5. Monitor database
vi ~/monitor.sh
# Add:
#!/bin/bash
while true; do
    clear
    echo "=== GEX Platform Status ==="
    echo ""
    
    echo "Services:"
    systemctl is-active gex-ingestion | xargs echo "  Ingestion:"
    systemctl is-active gex-scheduler | xargs echo "  Scheduler:"
    systemctl is-active postgresql | xargs echo "  PostgreSQL:"
    echo ""
    
    echo "Recent Data (last 5 minutes):"
    psql -U gex_user -d gex_db -h localhost << EOF
    SELECT 
        'Options:' as type,
        COUNT(*) as count,
        MAX(timestamp) as latest
    FROM options_quotes 
    WHERE timestamp > NOW() - INTERVAL '5 minutes'
    UNION ALL
    SELECT 
        'GEX:' as type,
        COUNT(*) as count,
        MAX(timestamp) as latest
    FROM gex_metrics 
    WHERE timestamp > NOW() - INTERVAL '5 minutes';
EOF
    echo ""
    
    echo "Disk Usage:"
    df -h / | tail -1
    echo ""
    
    echo "Memory Usage:"
    free -h | grep Mem
    echo ""
    
    sleep 10
done

6. Make executable
chmod +x ~/monitor.sh

7. Run monitor
./monitor.sh

##########################################################################################
### Step 8: Set Up Automated Backups

1. Create .pgpass file in ubuntu's home directory
cat > ~/.pgpass << 'EOF'
localhost:5432:gex_db:gex_user:your_secure_password_here
EOF
# Then, replace placeholder with actual password:
vi ~/.pgpass

2. Set strict permissions (required by PostgreSQL)
chmod 600 ~/.pgpass

3. Test it works
pg_dump -U gex_user -h localhost gex_db | head -n 20

4. Create DB backup script
sudo vi /usr/local/bin/backup-gex-db.sh

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

# Backup database
echo "[$(date)] Starting backup..."

if pg_dump -U "$DB_USER" -h "$DB_HOST" "$DB_NAME" | gzip > "$BACKUP_DIR/${DB_NAME}_${DATE}.sql.gz"; then
    echo "[$(date)] ✅ Backup completed: ${DB_NAME}_${DATE}.sql.gz"
    
    # Get backup size
    SIZE=$(du -h "$BACKUP_DIR/${DB_NAME}_${DATE}.sql.gz" | cut -f1)
    echo "[$(date)] Backup size: $SIZE"
    
    # Clean up old backups
    DELETED=$(find "$BACKUP_DIR" -name "${DB_NAME}_*.sql.gz" -mtime +$RETENTION_DAYS -delete -print | wc -l)
    if [ "$DELETED" -gt 0 ]; then
        echo "[$(date)] Deleted $DELETED old backup(s)"
    fi
    
    # List current backups
    echo "[$(date)] Current backups:"
    ls -lh "$BACKUP_DIR/${DB_NAME}_"*.sql.gz | tail -5
    
else
    echo "[$(date)] ❌ Backup failed!"
    exit 1
fi

echo "[$(date)] Backup process complete"

5. Make it executable
sudo chmod +x /usr/local/bin/backup-gex-db.sh

6. Test the backup script
# Run manually to test
sudo -u ubuntu /usr/local/bin/backup-gex-db.sh
# Check the backup was created
ls -lh /home/ubuntu/backups/

7. Setup automated backups via crontab
crontab -e
# Add:
0 2 * * * /usr/local/bin/backup-gex-db.sh >> /home/ubuntu/backup.log 2>&1

##########################################################################################
### Step 9: Security Hardening

1. Set up firewall
sudo ufw allow 22/tcp  # SSH
sudo ufw enable

2. Disable root login
sudo vi /etc/ssh/sshd_config
# Change:
PermitRootLogin no
PasswordAuthentication no

3. Restart SSHD
sudo systemctl restart sshd

4. Set up automatic security updates
sudo apt install unattended-upgrades
sudo dpkg-reconfigure -plow unattended-upgrades

##########################################################################################
### Step 10: Optional - Set Up CloudWatch Monitoring

1. Install CloudWatch agent
wget https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb
sudo dpkg -i amazon-cloudwatch-agent.deb

2. Configure CloudWatch
sudo vi /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
# Add:
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/syslog",
            "log_group_name": "/gex/syslog",
            "log_stream_name": "{instance_id}"
          }
        ]
      }
    }
  },
  "metrics": {
    "namespace": "GEX/Platform",
    "metrics_collected": {
      "mem": {
        "measurement": [
          "mem_used_percent"
        ]
      },
      "disk": {
        "measurement": [
          "used_percent"
        ],
        "resources": [
          "/"
        ]
      }
    }
  }
}

3. Start CloudWatch agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config \
    -m ec2 \
    -s \
    -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

##########################################################################################
### Step 11: Quick Deployment Script

1. Create deploy/deploy.sh for easy redeployment:
cat > deploy/deploy.sh << 'EOF'
#!/bin/bash
set -e

echo "🚀 Deploying GEX Options Platform..."

# Pull latest code
cd /home/ubuntu/gex-options-platform
git pull origin main

# Activate virtual environment
source venv/bin/activate

# Install/update dependencies
pip install -r requirements.txt

# Restart services
sudo systemctl restart gex-ingestion
sudo systemctl restart gex-scheduler

echo "✅ Deployment complete!"

# Show status
sudo systemctl status gex-ingestion --no-pager
sudo systemctl status gex-scheduler --no-pager
EOF

chmod +x deploy/deploy.sh

##########################################################################################
### Step 12: Test Deployment

1. Test database connection
psql -U gex_user -d gex_db -h localhost -c "SELECT NOW();"

2. Test ingestion (check logs)
sudo journalctl -u gex-ingestion -n 50

3. Check if data is flowing
psql -U gex_user -d gex_db -h localhost << EOF
SELECT COUNT(*) as options_count, MAX(timestamp) as latest
FROM options_quotes
WHERE timestamp > NOW() - INTERVAL '10 minutes';
EOF

4. Check GEX calculations
psql -U gex_user -d gex_db -h localhost << EOF
SELECT * FROM latest_gex LIMIT 5;
EOF

##########################################################################################
### Troubleshooting

1. If ingestion service fails:
# Check logs
sudo journalctl -u gex-ingestion -n 100 --no-pager

# Check if TradeStation credentials work
cd /home/ubuntu/gex-options-platform
source venv/bin/activate
python src/ingestion/tradestation_auth.py

# Restart service
sudo systemctl restart gex-ingestion

2. If database connection fails:
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check connections
sudo -u postgres psql -c "\conninfo"

# Check pg_hba.conf
sudo cat /etc/postgresql/14/main/pg_hba.conf | grep gex
