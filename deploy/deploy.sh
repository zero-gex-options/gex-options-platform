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
