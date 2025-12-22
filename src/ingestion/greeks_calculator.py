"""
Options Greeks Calculator using Black-Scholes model

Standalone module for calculating options Greeks.
"""

import numpy as np
from scipy.stats import norm
from datetime import datetime, time as dt_time, date
import pytz

class GreeksCalculator:
    """Calculate options Greeks using Black-Scholes model with dividends"""
    
    def __init__(self, risk_free_rate=0.045, dividend_yield=0.013):
        """
        Args:
            risk_free_rate: Annual risk-free rate (default 4.5% current 10Y treasury)
            dividend_yield: Annual dividend yield (default 1.3% for SPY)
        """
        self.risk_free_rate = risk_free_rate
        self.dividend_yield = dividend_yield
        
    def calculate_greeks(self, underlying_price, strike, expiration, 
                        option_type, implied_vol, current_time=None):
        """
        Calculate all Greeks for an option using Black-Scholes with dividends
        
        Args:
            underlying_price: Current price of underlying
            strike: Strike price
            expiration: Expiration date (datetime.date or datetime)
            option_type: 'call' or 'put'
            implied_vol: Implied volatility (as decimal, e.g., 0.20 for 20%)
            current_time: Current time (defaults to now)
            
        Returns:
            dict with delta, gamma, theta, vega, rho
        """
        if current_time is None:
            current_time = datetime.now(pytz.timezone('America/New_York'))
        
        # Calculate time to expiration in years
        T = self._time_to_expiration(current_time, expiration)
        
        # Handle expired/same-day options
        if T <= 0:
            return self._expired_greeks(underlying_price, strike, option_type)
        
        # For very short DTE (< 1 hour), add minimum time to avoid numerical issues
        if T < 1/365/24:  # Less than 1 hour
            T = max(T, 1/365/24)  # Minimum 1 hour
        
        S = underlying_price
        K = strike
        sigma = implied_vol
        r = self.risk_free_rate
        q = self.dividend_yield  # Dividend yield
        
        # Black-Scholes with dividends: d1 and d2
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        # Calculate Greeks
        if option_type.lower() == 'call':
            # Call option Greeks
            delta = np.exp(-q * T) * norm.cdf(d1)
            theta = ((-S * norm.pdf(d1) * sigma * np.exp(-q * T) / (2 * np.sqrt(T)) 
                     - r * K * np.exp(-r * T) * norm.cdf(d2)
                     + q * S * np.exp(-q * T) * norm.cdf(d1)) / 365)
            rho = K * T * np.exp(-r * T) * norm.cdf(d2) / 100
        else:  # put
            # Put option Greeks
            delta = -np.exp(-q * T) * norm.cdf(-d1)
            theta = ((-S * norm.pdf(d1) * sigma * np.exp(-q * T) / (2 * np.sqrt(T)) 
                     + r * K * np.exp(-r * T) * norm.cdf(-d2)
                     - q * S * np.exp(-q * T) * norm.cdf(-d1)) / 365)
            rho = -K * T * np.exp(-r * T) * norm.cdf(-d2) / 100
        
        # Gamma and Vega are same for calls and puts (with dividend adjustment)
        gamma = norm.pdf(d1) * np.exp(-q * T) / (S * sigma * np.sqrt(T))
        vega = S * np.exp(-q * T) * norm.pdf(d1) * np.sqrt(T) / 100
        
        return {
            'delta': round(delta, 6),
            'gamma': round(gamma, 8),
            'theta': round(theta, 6),
            'vega': round(vega, 6),
            'rho': round(rho, 6)
        }

    def _time_to_expiration(self, current_time, expiration):
        """Calculate time to expiration in years"""
        
        # Convert expiration to datetime if it's a date
        if isinstance(expiration, date) and not isinstance(expiration, datetime):
            # Options expire at 4:00 PM ET
            exp_datetime = datetime.combine(




                expiration, 
                dt_time(16, 0)
            ).replace(tzinfo=pytz.timezone('America/New_York'))
        else:
            exp_datetime = expiration
        
        # Ensure current_time is timezone-aware
        if current_time.tzinfo is None:
            current_time = pytz.timezone('America/New_York').localize(current_time)
        
        # Time difference in years
        time_diff = (exp_datetime - current_time).total_seconds()
        T = time_diff / (365.25 * 24 * 3600)
        
        return max(T, 0)
    
    def _expired_greeks(self, underlying_price, strike, option_type):
        """Greeks for expired/worthless options"""
        is_itm = (underlying_price > strike if option_type.lower() == 'call' 
                 else underlying_price < strike)
        
        return {
            'delta': 1.0 if is_itm else 0.0,
            'gamma': 0.0,
            'theta': 0.0,
            'vega': 0.0,
            'rho': 0.0
        }
    
    def implied_vol_from_price(self, price, underlying_price, strike, 
                               expiration, option_type, current_time=None):
        """
        Back out implied volatility from option price using bisection
        
        Args:
            price: Market price of the option
            underlying_price: Current underlying price
            strike: Strike price
            expiration: Expiration date
            option_type: 'call' or 'put'
            current_time: Current time (defaults to now)
            
        Returns:
            Implied volatility (decimal) or None if can't solve
        """
        from scipy.optimize import brentq
        
        if current_time is None:
            current_time = datetime.now(pytz.timezone('America/New_York'))
        
        T = self._time_to_expiration(current_time, expiration)
        
        if T <= 0:
            return None
        
        def objective(sigma):
            """Calculate difference between theoretical and market price"""
            if sigma <= 0:
                return float('inf')
            
            S = underlying_price
            K = strike
            r = self.risk_free_rate
            
            d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
            d2 = d1 - sigma * np.sqrt(T)
            
            if option_type.lower() == 'call':
                theo_price = (S * norm.cdf(d1) - 
                            K * np.exp(-r * T) * norm.cdf(d2))
            else:
                theo_price = (K * np.exp(-r * T) * norm.cdf(-d2) - 
                            S * norm.cdf(-d1))
            
            return theo_price - price
        
        try:
            # Search for IV between 1% and 500%
            iv = brentq(objective, 0.01, 5.0)
            return round(iv, 6)
        except:
            return None


# Test function
def test_greeks():
    """Test the Greeks calculator"""
    
    print("="*60)
    print("Testing Greeks Calculator")
    print("="*60)
    
    calc = GreeksCalculator(risk_free_rate=0.05)
    
    # Test 1: ATM call option
    print("\nTest 1: ATM Call Option")
    print("-"*40)
    greeks = calc.calculate_greeks(
        underlying_price=600.00,
        strike=600.00,
        expiration=date.today(),
        option_type='call',
        implied_vol=0.15
    )
    
    print("SPY $600 Call (ATM, 0DTE, 15% IV):")
    for key, value in greeks.items():
        print(f"  {key}: {value}")
    
    # Test 2: OTM put option
    print("\nTest 2: OTM Put Option")
    print("-"*40)
    greeks = calc.calculate_greeks(
        underlying_price=600.00,
        strike=590.00,
        expiration=date.today(),
        option_type='put',
        implied_vol=0.20
    )
    
    print("SPY $590 Put (OTM, 0DTE, 20% IV):")
    for key, value in greeks.items():
        print(f"  {key}: {value}")
    
    # Test 3: IV calculation
    print("\nTest 3: Implied Volatility from Price")
    print("-"*40)
    
    iv = calc.implied_vol_from_price(
        price=3.00,
        underlying_price=600.00,
        strike=600.00,
        expiration=date.today(),
        option_type='call'
    )
    
    if iv:
        print(f"Option Price: $3.00")
        print(f"Implied Vol: {iv:.4f} ({iv*100:.2f}%)")
    else:
        print("Could not calculate IV")
    
    print("\n" + "="*60)
    print("✅ All tests completed!")
    print("="*60)


if __name__ == '__main__':
    test_greeks()
