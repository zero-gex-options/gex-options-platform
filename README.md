## GEX Options Analytics Platform  
  
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
```  

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
├── deploy/  
│   ├── deploy.sh  
│   └── steps/  
│       ├── 010.setup  
│       ├── 020.database  
│       ├── 030.application  
│       ├── 040.tokens  
│       ├── 050.security  
│       ├── 060.backups  
│       ├── 070.systemd  
│       ├── 080.validation  
│       └── 090.monitoring  
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
├── tests/  
│   ├── __init__.py  
│   ├── debug_market_hours.py  
│   ├── test_db_write.py  
│   ├── test_env.sh  
│   ├── test_full_pipeline.py  
│   ├── test_greeks_comparison.py  
│   ├── test_ingestion_debug.sh  
│   └── view_data.sh  
│  
├── deployment/  
│   ├── database/  
│   │   ├── backup-gex-db.sh  
│   │   └── restore-gex-db.sh  
│   │  
│   ├── run/  
│   │   ├── ingestion.sh  
│   │   └── scheduler.sh  
│   │  
│   └── systemd/  
│       ├── gex-ingestion.service  
│       ├── gex-scheduler.service  
│       ├── gex-monitor.service  
│       └── gex-dashboard.service  
│  
├── monitoring/  
│   ├── monitor.py  
│   ├── dashboard.py  
│   └── dashboard.html  
│  
├── branding/  
│   └── ZeroGEX.png  
│  
└── initialize/  
    └── get_tradestation_tokens.py  
```
  
  
## Status  
  
🚧 Active Development  
  
- [x] Data ingestion (TradeStation)  
- [x] Greeks calculation  
- [ ] GEX calculation engine (in progress)  
- [ ] Dashboard  
- [ ] ML pattern detection  
  
  
## Prerequisites  
  
- Python 3.9+  
- PostgreSQL 14+ with TimescaleDB  
- TradeStation API account  
  
  
## Initial Setup  
  
1. Setup SSH keypair  
```
ssh-keygen -t ed25519 -C "zerogexoptions@gmail.com"
chmod 0600 .ssh/id_ed25519
cat .ssh/id_ed25519.pub
# Add new SSH key in GitHub and copy/paste public key
```
  
2. Clone repo and update git config with author/email
```
git clone git@github.com:zero-gex-options/gex-options-platform.git  
cd gex-options-platform  
git config --global --edit
```
  
3. Run deploy script  
```
./deploy/deploy.sh  
```
  
  
## License  
  
MIT  
  
  
## Disclaimer  
  
Educational purposes only. Not financial advice.  
=======  
# gex-options-platform  
