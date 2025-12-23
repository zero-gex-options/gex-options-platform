#!/bin/bash
cd /home/ubuntu/gex-options-platform
source venv/bin/activate
export $(grep -v '^#' .env | xargs)
exec python src/gex/gex_scheduler.py
