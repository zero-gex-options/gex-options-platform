import os
import sys
from datetime import date

# Add parent directory to path 
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.ingestion.greeks_calculator import GreeksCalculator

calc = GreeksCalculator(risk_free_rate=0.045, dividend_yield=0.013)

# Test case: ATM 0DTE call
spot = 684.0
strike = 684.0
iv = 0.15

greeks = calc.calculate_greeks(
    underlying_price=spot,
    strike=strike,
    expiration=date.today(),
    option_type='call',
    implied_vol=iv
)

print("Calculated Greeks for ATM 0DTE Call:")
print(f"  Spot: ${spot}, Strike: ${strike}, IV: {iv*100:.1f}%")
print(f"  Delta: {greeks['delta']:.4f}")
print(f"  Gamma: {greeks['gamma']:.6f}")
print(f"  Theta: {greeks['theta']:.4f}")
print(f"  Vega: {greeks['vega']:.4f}")
