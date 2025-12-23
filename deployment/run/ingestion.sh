#!/bin/bash

# Navigate to project directory
cd /home/ubuntu/gex-options-platform

# Activate virtual environment
source venv/bin/activate

# Load environment variables from .env
export $(grep -v '^#' .env | xargs)

# Run the ingestion engine
exec python src/ingestion/tradestation_streaming_ingestion_engine.py
