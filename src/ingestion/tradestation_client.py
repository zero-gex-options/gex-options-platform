"""
TradeStation Streaming Client

Handles streaming market data from TradeStation API.
"""

import asyncio
import aiohttp
import json
from typing import Dict, List, Optional, Callable, Any
import logging
import os
from dotenv import load_dotenv
from tradestation_auth import TradeStationAuth

load_dotenv()

# Get and validate logging level from environment
log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
valid_levels = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

if log_level_str in valid_levels:
    log_level = valid_levels[log_level_str]
else:
    log_level = logging.INFO
    print(f"Warning: Invalid LOG_LEVEL '{log_level_str}', defaulting to INFO. Valid options: {', '.join(valid_levels.keys())}")

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradeStationStreamingClient:
    """Async client for TradeStation streaming API"""
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize streaming client
        
        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: Refresh token
            sandbox: Use sandbox environment
        """
        logger.debug(f"Initializing TradeStationStreamingClient (sandbox={sandbox})")
        
        self.auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)
        self.sandbox = sandbox
        
        if sandbox:
            logger.warning("Using SANDBOX environment - data may not be real-time")
            self.base_url = "https://sim-api.tradestation.com/v3"
            self.stream_url = "https://sim-api.tradestation.com/v3/marketdata/stream/options/quotes"
        else:
            logger.info("Using PRODUCTION environment")
            self.base_url = "https://api.tradestation.com/v3"
            self.stream_url = "https://api.tradestation.com/v3/marketdata/stream/options/quotes"
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.stream_task: Optional[asyncio.Task] = None
        
        logger.info("TradeStation streaming client initialized")
    
    async def __aenter__(self):
        """Context manager entry"""
        logger.debug("Entering TradeStationStreamingClient context")
        self.session = aiohttp.ClientSession()
        logger.debug("Created aiohttp session")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        logger.debug("Exiting TradeStationStreamingClient context")
        
        if self.stream_task and not self.stream_task.done():
            logger.info("Cancelling stream task...")
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                logger.debug("Stream task cancelled successfully")
        
        if self.session:
            logger.debug("Closing aiohttp session")
            await self.session.close()
        
        logger.info("TradeStation client closed")
    
    async def get_quote(self, symbol: str) -> Optional[Dict]:
        """
        Get current quote for symbol
        
        Args:
            symbol: Symbol to quote
            
        Returns:
            Quote data or None
        """
        logger.debug(f"Requesting quote for {symbol}")
        
        url = f"{self.base_url}/marketdata/quotes/{symbol}"
        headers = self.auth.get_headers()
        
        try:
            async with self.session.get(url, headers=headers, timeout=10) as response:
                logger.debug(f"Quote request status: {response.status}")
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Quote request failed with status {response.status}: {error_text}")
                    return None
                
                data = await response.json()
                
                if 'Quotes' not in data or len(data['Quotes']) == 0:
                    logger.warning(f"No quote data returned for {symbol}")
                    return None
                
                quote = data['Quotes'][0]
                price = float(quote.get('Last', 0))
                
                logger.info(f"✅ {symbol}: ${price:.2f}")
                logger.debug(f"Full quote data: Bid=${quote.get('Bid')}, Ask=${quote.get('Ask')}, Volume={quote.get('Volume')}")
                
                return {
                    'symbol': symbol,
                    'price': price,
                    'bid': float(quote.get('Bid', 0)),
                    'ask': float(quote.get('Ask', 0)),
                    'volume': int(quote.get('Volume', 0)),
                    'timestamp': quote.get('TradeTime')
                }
                
        except asyncio.TimeoutError:
            logger.error(f"Quote request timed out for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error getting quote for {symbol}: {e}", exc_info=True)
            return None
    
    async def get_option_expirations(self, underlying: str) -> List[str]:
        """
        Get available option expiration dates
        
        Args:
            underlying: Underlying symbol
            
        Returns:
            List of expiration dates (YYYY-MM-DD)
        """
        logger.debug(f"Requesting option expirations for {underlying}")
        
        url = f"{self.base_url}/marketdata/options/expirations/{underlying}"
        headers = self.auth.get_headers()
        
        try:
            async with self.session.get(url, headers=headers, timeout=10) as response:
                logger.debug(f"Expirations request status: {response.status}")
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Expirations request failed: {error_text}")
                    return []
                
                data = await response.json()
                expirations = data.get('Expirations', [])
                
                logger.info(f"✅ Found {len(expirations)} expirations for {underlying}")
                logger.debug(f"Expirations: {expirations[:5]}..." if len(expirations) > 5 else f"Expirations: {expirations}")
                
                return expirations
                
        except asyncio.TimeoutError:
            logger.error(f"Expirations request timed out for {underlying}")
            return []
        except Exception as e:
            logger.error(f"Error getting expirations for {underlying}: {e}", exc_info=True)
            return []
    
    async def stream_options(self, underlying: str, expiration: str, handler: Callable[[Dict], Any]):
        """
        Stream option quotes for given underlying and expiration
        
        Args:
            underlying: Underlying symbol (e.g., 'SPY')
            expiration: Expiration date (YYYY-MM-DD)
            handler: Async callback function to handle each update
        """
        logger.info(f"🚀 Starting options stream for {underlying} {expiration}")
        
        # Get fresh token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'
        
        payload = {
            "UnderlyingSymbol": underlying,
            "Expirations": [expiration]
        }
        
        logger.debug(f"Stream request payload: {payload}")
        logger.debug(f"Stream URL: {self.stream_url}")
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                logger.debug(f"Initiating stream connection (attempt {retry_count + 1}/{max_retries})")
                
                async with self.session.post(
                    self.stream_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=None)  # No timeout for streaming
                ) as response:
                    
                    logger.debug(f"Stream response status: {response.status}")
                    
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Stream connection failed with status {response.status}: {error_text}")
                        
                        if response.status in [401, 403]:
                            logger.warning("Authentication failed, refreshing token...")
                            headers = self.auth.get_headers()
                            headers['Content-Type'] = 'application/json'
                        
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 2 ** retry_count
                            logger.info(f"Retrying in {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.critical("Max retries reached, giving up on stream connection")
                            return
                    
                    logger.info("✅ Stream connection established")
                    retry_count = 0  # Reset on successful connection
                    
                    message_count = 0
                    heartbeat_count = 0
                    error_count = 0
                    
                    async for line in response.content:
                        try:
                            if not line:
                                continue
                            
                            line_str = line.decode('utf-8').strip()
                            
                            if not line_str:
                                continue
                            
                            logger.debug(f"Raw stream data: {line_str[:200]}...")
                            
                            # Handle heartbeats
                            if line_str == 'Heartbeat':
                                heartbeat_count += 1
                                if heartbeat_count % 10 == 0:
                                    logger.debug(f"Received {heartbeat_count} heartbeats")
                                continue
                            
                            # Parse JSON
                            try:
                                data = json.loads(line_str)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse JSON: {e}")
                                logger.debug(f"Invalid JSON: {line_str}")
                                error_count += 1
                                if error_count > 10:
                                    logger.error("Too many JSON parse errors, may indicate stream issue")
                                continue
                            
                            message_count += 1
                            
                            if message_count % 100 == 0:
                                logger.info(f"Stream Message #{message_count} (heartbeats: {heartbeat_count})")
                            else:
                                logger.debug(f"Stream Message #{message_count}")
                            
                            # Call handler
                            try:
                                if asyncio.iscoroutinefunction(handler):
                                    await handler(data)
                                else:
                                    handler(data)
                            except Exception as e:
                                logger.error(f"Error in stream handler: {e}", exc_info=True)
                                
                        except Exception as e:
                            logger.error(f"Error processing stream line: {e}", exc_info=True)
                    
                    logger.warning(f"Stream ended after {message_count} messages and {heartbeat_count} heartbeats")
                    
            except asyncio.CancelledError:
                logger.info("Stream cancelled")
                raise
            except asyncio.TimeoutError:
                logger.error("Stream connection timed out")
                retry_count += 1
            except Exception as e:
                logger.error(f"Stream error: {e}", exc_info=True)
                retry_count += 1
            
            if retry_count < max_retries:
                wait_time = 2 ** retry_count
                logger.info(f"Reconnecting in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
        
        logger.critical(f"Stream failed after {max_retries} retries")


# Test
async def main():
    logger.info("Testing TradeStation streaming client...")
    
    async with TradeStationStreamingClient(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    ) as client:
        
        # Test quote
        quote = await client.get_quote('SPY')
        if quote:
            logger.info(f"Quote test successful: {quote}")
        
        # Test expirations
        exps = await client.get_option_expirations('SPY')
        if exps:
            logger.info(f"Expirations test successful: {len(exps)} found")


if __name__ == '__main__':
    asyncio.run(main())
