#!/bin/bash
set -a
source /home/ubuntu/gex-options-platform/.env
set +a
echo "DB_HOST=$DB_HOST"
echo "DB_USER=$DB_USER"
echo "TRADESTATION_CLIENT_ID=${TRADESTATION_CLIENT_ID:0:10}..."
