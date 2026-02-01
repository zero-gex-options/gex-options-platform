# GEX Module

Gamma Exposure (GEX) calculation and analysis for options trading.

## Overview

The GEX module calculates dealer gamma exposure from options market data and provides insights into market dynamics. Dealer gamma positioning significantly impacts price action:

- **Positive Gamma**: Dealers hedge by buying dips and selling rallies (stabilizing)
- **Negative Gamma**: Dealers amplify moves by selling dips and buying rallies (destabilizing)

## Module Structure

```
src/gex/
├── __init__.py              # Module exports
├── gex_metrics.py           # Data structures (GEXMetrics, StrikeGammaProfile)
├── gex_calculator.py        # Core GEX calculation engine
├── gex_scheduler.py         # Automated calculation scheduler
├── gex_analyzer.py          # Advanced analysis and insights
├── gex_cli.py               # Command-line interface
└── README.md                # This file
```

## Components

### GEXCalculator

Core calculation engine that computes gamma exposure metrics from options data.

**Features:**
- Calculates total gamma exposure (calls + puts)
- Computes net dealer GEX (calls - puts)
- Identifies max gamma strikes
- Finds gamma flip points (zero-gamma levels)
- Tracks put/call ratios and higher-order Greeks

**Usage:**
```python
from src.gex import GEXCalculator
import psycopg2

db = psycopg2.connect(...)
calculator = GEXCalculator(db)

# Calculate current GEX
metrics = calculator.calculate_current_gex('SPY', current_price=600.00)

print(f"Total GEX: ${metrics.total_gex_millions:.1f}M")
print(f"Net GEX: ${metrics.net_gex_millions:.1f}M")
print(f"Regime: {metrics.gamma_regime}")
```

### GEXScheduler

Automated scheduler that runs GEX calculations periodically during market hours.

**Features:**
- Market hours detection (9:30 AM - 4:00 PM ET)
- Fresh price fetching from TradeStation
- Configurable calculation intervals
- Multi-symbol support
- Statistics tracking

**Usage:**
```python
from src.gex import GEXScheduler
import asyncio

scheduler = GEXScheduler(
    interval_seconds=60,
    symbols=['SPY', 'QQQ'],
    target_expiration='today'
)

await scheduler.run()
```

**Command Line:**
```bash
# Run scheduler with default settings (SPY, 60s interval)
python src/gex/gex_scheduler.py

# Custom interval and symbols
python src/gex/gex_scheduler.py --interval 30 --symbols SPY QQQ --expiration today
```

### GEXAnalyzer

Advanced analysis tools for GEX data.

**Features:**
- Historical metrics retrieval
- Gamma regime change detection
- Key support/resistance level identification
- Expected move calculations
- Market state summaries

**Usage:**
```python
from src.gex import GEXAnalyzer

analyzer = GEXAnalyzer(db)

# Get current state summary
summary = analyzer.summarize_current_state('SPY')
print(summary)

# Find key gamma levels
levels = analyzer.find_key_gamma_levels('SPY', threshold_millions=50)
print(f"Support: {levels['support']}")
print(f"Resistance: {levels['resistance']}")

# Analyze regime changes
changes = analyzer.analyze_gamma_regime_changes('SPY', hours=24)

# Calculate expected move
move = analyzer.calculate_expected_move('SPY', confidence=0.68)
print(f"Range: ${move['expected_low']:.2f} - ${move['expected_high']:.2f}")
```

### Command-Line Interface

Powerful CLI for GEX analysis.

**Available Commands:**
```bash
# Calculate fresh GEX
python src/gex/gex_cli.py calculate SPY

# Show current summary
python src/gex/gex_cli.py summary SPY

# Find key gamma levels
python src/gex/gex_cli.py levels SPY --threshold 100

# Analyze regime changes
python src/gex/gex_cli.py regime SPY --hours 48

# Calculate expected move
python src/gex/gex_cli.py expected-move SPY --confidence 0.95

# View historical metrics
python src/gex/gex_cli.py history SPY --hours 24
```

## Data Structures

### GEXMetrics

Comprehensive container for GEX calculation results:

```python
@dataclass
class GEXMetrics:
    symbol: str
    expiration: date
    timestamp: datetime
    underlying_price: float
    
    # Gamma exposure
    total_gamma_exposure: float
    call_gamma: float
    put_gamma: float
    net_gex: float
    
    # Volume/OI
    call_volume: int
    put_volume: int
    call_oi: int
    put_oi: int
    total_contracts: int
    
    # Key levels
    max_gamma_strike: float
    max_gamma_value: float
    gamma_flip_point: Optional[float]
    
    # Indicators
    put_call_ratio: float
    vanna_exposure: float
    charm_exposure: float
    
    # Properties
    @property
    def is_positive_gamma_regime(self) -> bool
    
    @property
    def gamma_regime(self) -> str
```

### StrikeGammaProfile

Per-strike gamma exposure breakdown:

```python
@dataclass
class StrikeGammaProfile:
    strike: float
    call_gamma: float
    put_gamma: float
    net_gamma: float
    total_gamma: float
    call_oi: int
    put_oi: int
    call_volume: int
    put_volume: int
```

## Interpretation Guide

### Net GEX

- **Net GEX > 0**: Dealers are net long gamma
  - Stabilizing effect on price
  - Dealers buy dips, sell rallies
  - Lower volatility expected
  
- **Net GEX < 0**: Dealers are net short gamma
  - Destabilizing effect on price
  - Dealers sell dips, buy rallies
  - Higher volatility expected

### Max Gamma Strike

The strike with the highest total gamma exposure acts as a "magnet" for price, especially near expiration (0DTE).

### Gamma Flip Point

The price level where net GEX crosses zero:
- **Above flip**: Positive gamma regime (stabilizing)
- **Below flip**: Negative gamma regime (destabilizing)

## Integration with Platform

### Database Schema

GEX metrics are stored in the `gex_metrics` table:

```sql
CREATE TABLE gex_metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    expiration DATE NOT NULL,
    underlying_price DOUBLE PRECISION,
    total_gamma_exposure DOUBLE PRECISION,
    call_gamma DOUBLE PRECISION,
    put_gamma DOUBLE PRECISION,
    net_gex DOUBLE PRECISION,
    -- ... additional fields
    PRIMARY KEY (timestamp, symbol, expiration)
);
```

### Systemd Service

The GEX scheduler runs as a systemd service:

```bash
# Check status
sudo systemctl status gex-scheduler

# View logs
sudo journalctl -u gex-scheduler -f

# Restart
sudo systemctl restart gex-scheduler
```

## Examples

### Calculate and Store GEX

```python
from src.gex import GEXCalculator
from src.ingestion import TradeStationSimpleClient

# Get fresh price
ts_client = TradeStationSimpleClient(...)
quote = ts_client.get_quote('SPY')

# Calculate GEX
calculator = GEXCalculator(db)
metrics = calculator.calculate_current_gex('SPY', current_price=quote['close'])

# Metrics are automatically stored to database
```

### Analyze Market State

```python
from src.gex import GEXAnalyzer

analyzer = GEXAnalyzer(db)

# Current state
print(analyzer.summarize_current_state('SPY'))

# Key levels for entry/exit
levels = analyzer.find_key_gamma_levels('SPY')
print(f"Watch for support at: {levels['support'][0]}")
print(f"Watch for resistance at: {levels['resistance'][0]}")

# Expected volatility
move = analyzer.calculate_expected_move('SPY')
print(f"Expected range: ±{move['move_pct']:.1f}%")
```

### Monitor Regime Changes

```python
# Detect when market transitions between stabilizing/destabilizing
changes = analyzer.analyze_gamma_regime_changes('SPY', hours=24)

for change in changes:
    print(f"{change['timestamp']}: {change['from_regime']} → {change['to_regime']}")
    print(f"Price: ${change['price']:.2f}")
```

## Development

### Running Tests

```bash
# Test calculator
python src/gex/gex_calculator.py

# Test analyzer
python src/gex/gex_analyzer.py

# Test full pipeline
python tests/test_full_pipeline.py
```

### Adding New Metrics

1. Add fields to `GEXMetrics` dataclass in `gex_metrics.py`
2. Update calculation logic in `gex_calculator.py`
3. Update database schema if storing new metrics
4. Add analysis methods to `gex_analyzer.py` if needed

## References

- [Gamma Exposure and Market Dynamics](https://squeezemetrics.com/monitor/docs/gamma_exposure)
- [Understanding Dealer Positioning](https://spotgamma.com/gamma-exposure-explained/)

## Notes

- GEX calculations require options data with Greeks (delta, gamma, etc.)
- Most accurate for 0DTE (same-day expiration) options
- Market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
- Fresh underlying prices improve calculation accuracy

## License

MIT License - See main project LICENSE file
