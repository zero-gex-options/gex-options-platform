"""
GEX Metrics Data Structures

Defines data classes and structures for gamma exposure metrics.
"""

from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Dict


@dataclass
class StrikeGammaProfile:
    """Gamma profile for a specific strike"""
    strike: float
    call_gamma: float
    put_gamma: float
    net_gamma: float
    total_gamma: float
    call_oi: int
    put_oi: int
    call_volume: int
    put_volume: int
    
    @property
    def gamma_exposure_millions(self) -> float:
        """Total gamma exposure in millions"""
        return self.total_gamma / 1e6
    
    @property
    def net_exposure_millions(self) -> float:
        """Net gamma exposure in millions"""
        return self.net_gamma / 1e6


@dataclass
class GEXMetrics:
    """Container for comprehensive GEX calculation results"""
    
    # Identification
    symbol: str
    expiration: date
    timestamp: datetime
    
    # Market state
    underlying_price: float
    
    # Aggregate gamma exposure
    total_gamma_exposure: float      # Total absolute gamma (calls + puts)
    call_gamma: float                # Total call gamma exposure
    put_gamma: float                 # Total put gamma exposure
    net_gex: float                   # Net gamma (calls - puts, dealer perspective)
    
    # Volume and open interest
    call_volume: int
    put_volume: int
    call_oi: int
    put_oi: int
    total_contracts: int
    
    # Key levels
    max_gamma_strike: float          # Strike with highest total gamma
    max_gamma_value: float           # Gamma value at max strike
    gamma_flip_point: Optional[float]  # Where net GEX crosses zero
    
    # Ratios and indicators
    put_call_ratio: float            # Put OI / Call OI
    
    # Higher order Greeks
    vanna_exposure: float            # Sensitivity to vol and spot
    charm_exposure: float            # Gamma decay over time
    
    # Strike-level details (optional)
    strike_profiles: Optional[Dict[float, StrikeGammaProfile]] = None
    
    def __post_init__(self):
        """Validate metrics after initialization"""
        if self.underlying_price <= 0:
            raise ValueError(f"Invalid underlying price: {self.underlying_price}")
        
        if self.total_gamma_exposure < 0:
            raise ValueError(f"Total gamma exposure cannot be negative: {self.total_gamma_exposure}")
    
    @property
    def total_gex_millions(self) -> float:
        """Total gamma exposure in millions"""
        return self.total_gamma_exposure / 1e6
    
    @property
    def net_gex_millions(self) -> float:
        """Net gamma exposure in millions"""
        return self.net_gex / 1e6
    
    @property
    def call_gamma_millions(self) -> float:
        """Call gamma in millions"""
        return self.call_gamma / 1e6
    
    @property
    def put_gamma_millions(self) -> float:
        """Put gamma in millions"""
        return self.put_gamma / 1e6
    
    @property
    def is_positive_gamma_regime(self) -> bool:
        """
        True if dealers are net long gamma (price should be stable).
        This occurs when net_gex > 0 (calls > puts from dealer perspective).
        """
        return self.net_gex > 0
    
    @property
    def gamma_regime(self) -> str:
        """Return human-readable gamma regime"""
        if self.is_positive_gamma_regime:
            return "Positive (Stabilizing)"
        else:
            return "Negative (Destabilizing)"
    
    def get_strike_profile(self, strike: float) -> Optional[StrikeGammaProfile]:
        """Get gamma profile for a specific strike"""
        if self.strike_profiles:
            return self.strike_profiles.get(strike)
        return None
    
    def to_dict(self) -> dict:
        """Convert metrics to dictionary for storage/serialization"""
        return {
            'symbol': self.symbol,
            'expiration': self.expiration.isoformat(),
            'timestamp': self.timestamp.isoformat(),
            'underlying_price': self.underlying_price,
            'total_gamma_exposure': self.total_gamma_exposure,
            'call_gamma': self.call_gamma,
            'put_gamma': self.put_gamma,
            'net_gex': self.net_gex,
            'call_volume': self.call_volume,
            'put_volume': self.put_volume,
            'call_oi': self.call_oi,
            'put_oi': self.put_oi,
            'total_contracts': self.total_contracts,
            'max_gamma_strike': self.max_gamma_strike,
            'max_gamma_value': self.max_gamma_value,
            'gamma_flip_point': self.gamma_flip_point,
            'put_call_ratio': self.put_call_ratio,
            'vanna_exposure': self.vanna_exposure,
            'charm_exposure': self.charm_exposure
        }
    
    def summary(self) -> str:
        """Return human-readable summary of GEX metrics"""
        lines = [
            f"GEX Metrics for {self.symbol} (Exp: {self.expiration})",
            f"  Timestamp: {self.timestamp}",
            f"  Spot Price: ${self.underlying_price:.2f}",
            f"  ",
            f"  Total GEX: ${self.total_gex_millions:.1f}M",
            f"  Net GEX: ${self.net_gex_millions:.1f}M",
            f"  Call Gamma: ${self.call_gamma_millions:.1f}M",
            f"  Put Gamma: ${self.put_gamma_millions:.1f}M",
            f"  ",
            f"  Gamma Regime: {self.gamma_regime}",
            f"  Max Gamma Strike: ${self.max_gamma_strike:.2f}",
            f"  Put/Call Ratio: {self.put_call_ratio:.2f}",
            f"  Total Contracts: {self.total_contracts:,}",
        ]
        
        if self.gamma_flip_point:
            lines.append(f"  Gamma Flip Point: ${self.gamma_flip_point:.2f}")
        
        return "\n".join(lines)
