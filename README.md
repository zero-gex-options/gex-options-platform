# GEX Options Analytics Platform

Real-time gamma exposure (GEX) analysis for 0DTE SPY options.

## Features

- Real-time options data streaming from TradeStation
- Greeks calculation using Black-Scholes model
- Gamma exposure analytics
- TimescaleDB time-series storage
- Production-ready error handling

## Architecture
```
Real-time Data → Ingestion Layer → Storage → Calculation Engine → Dashboard
     ↓              ↓                  ↓            ↓                  ↓
  Market APIs   Python/Lambda    TimescaleDB    Real-time GEX      React/
  TradeStation  Event streams    PostgreSQL     Analytics          Plotly Dash
  CBOE/others   Kafka/Redis      S3 backup      Greeks calc        Cloud hosted

## Project Structure
```
gex-options-platform/
├── .gitignore
├── .env.example
├── README.md
├── LICENSE
├── requirements.txt
│
├── config/
│   └── database_schema.sql
│
├── src/
│   ├── __init__.py
│   │
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── greeks_calculator.py
│   │   ├── tradestation_auth.py
│   │   ├── tradestation_client.py
│   │   └── tradestation_streaming_ingestion_engine.py
│   │
│   ├── gex/
│   │   ├── __init__.py
│   │   ├── gex_calculator.py
│   │   └── gex_scheduler.py
│   │
│   └── dashboard/
│       └── __init__.py
│
├── scripts/
│   └── get_tradestation_tokens.py
│
├── tests/
│   ├── __init__.py
│   └── test_full_pipeline.py
│
└── deployment/
    └── systemd/
        └── gex-ingestion.service


## Status

🚧 Active Development

- [x] Data ingestion (TradeStation)
- [x] Greeks calculation
- [ ] GEX calculation engine (in progress)
- [ ] Dashboard
- [ ] ML pattern detection

## Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL 14+ with TimescaleDB
- TradeStation API account

### Installation
```bash
# Clone and setup
git clone <your-repo>
cd gex-options-platform

# Virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Database setup
psql -U postgres -f config/database_schema.sql

# Configure
cp .env.example .env
# Edit .env with your credentials
```

### Get TradeStation Tokens
```bash
python scripts/get_tradestation_tokens.py
```

### Run
```bash
python src/ingestion/tradestation_streaming_ingestion_engine.py
```

## License

MIT

## Disclaimer

Educational purposes only. Not financial advice.
=======
# gex-options-platform
