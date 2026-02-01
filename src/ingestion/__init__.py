"""
Ingestion Module

This module handles real-time options data ingestion from TradeStation API.

Components:
    - tradestation_auth: OAuth2 authentication management
    - tradestation_client: REST API client for market data
    - tradestation_streaming_client: WebSocket streaming client
    - streaming_ingestion_engine: Main data ingestion pipeline
    - greeks_calculator: Black-Scholes Greeks calculation

Usage:
    # REST API Client
    from src.ingestion import TradeStationSimpleClient
    client = TradeStationSimpleClient(client_id, secret, refresh_token)
    quote = client.get_quote('SPY')
    
    # Streaming Client
    from src.ingestion import TradeStationStreamingClient
    async with TradeStationStreamingClient(...) as client:
        await client.stream_options_chain(...)
    
    # Greeks Calculator
    from src.ingestion import GreeksCalculator
    calc = GreeksCalculator(risk_free_rate=0.045)
    greeks = calc.calculate_greeks(...)
    
    # Ingestion Engine (main pipeline)
    from src.ingestion import StreamingIngestionEngine
    engine = StreamingIngestionEngine()
    await engine.run()
"""

from .tradestation_auth import TradeStationAuth
from .tradestation_client import TradeStationSimpleClient
from .tradestation_streaming_client import TradeStationStreamingClient
from .streaming_ingestion_engine import StreamingIngestionEngine
from .greeks_calculator import GreeksCalculator

__all__ = [
    'TradeStationAuth',
    'TradeStationSimpleClient',
    'TradeStationStreamingClient',
    'StreamingIngestionEngine',
    'GreeksCalculator'
]

__version__ = '0.1.0'
