#!/bin/bash

# Navigate to project directory
cd /home/ubuntu/gex-options-platform

# Add project root to PYTHONPATH
export PYTHONPATH="/home/ubuntu/gex-options-platform:$PYTHONPATH"

# Activate virtual environment
source venv/bin/activate

# Load environment variables from .env
export $(grep -v '^#' .env | xargs)

# Run the streaming ingestion engine
exec python -m src.ingestion.streaming_ingestion_engine
