"""
TradeStation Streaming Client for Options Data

Streams options chains using the working endpoint:
/marketdata/stream/options/chains/{underlying}?expiration={date}
"""

import asyncio
import aiohttp
import json
from datetime import datetime, date
from typing import List, Dict, Optional, Callable
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import os
from dotenv import load_dotenv
from tradestation_auth import TradeStationAuth

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradeStationStreamingClient:
    """TradeStation streaming client for options data"""
    
    BASE_URL = "https://api.tradestation.com/v3"
    SANDBOX_URL = "https://sim-api.tradestation.com/v3"
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str,
                 sandbox: bool = False):
        self.base_url = self.SANDBOX_URL if sandbox else self.BASE_URL
        self.auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)
        self.sandbox = sandbox
        
        self.session = None
        self.is_streaming = False
        self.stream_task = None
        
        # Stats
        self.messages_received = 0
        self.errors_count = 0
        self.last_message_time = None
        
        logger.info(f"Initialized TradeStation Streaming Client (sandbox={sandbox})")
        
    async def __aenter__(self):
        """Async context manager entry"""
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=300)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop_streaming()
        if self.session:
            await self.session.close()
    
    async def get_quote(self, symbol: str) -> Dict:
        """Get current quote for underlying"""
        
        logger.info(f"Getting quote for {symbol}")
        endpoint = f"marketdata/quotes/{symbol}"
        url = f"{self.base_url}/{endpoint}"
        headers = self.auth.get_headers()
        
        async with self.session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            
            if 'Quotes' in data and len(data['Quotes']) > 0:
                quote = data['Quotes'][0]
                result = {
                    'symbol': symbol,
                    'price': float(quote.get('Last', 0)),
                    'bid': float(quote.get('Bid', 0)),
                    'ask': float(quote.get('Ask', 0)),
                    'volume': int(quote.get('Volume', 0)),
                    'timestamp': datetime.now()
                }
                logger.info(f"✅ {symbol}: ${result['price']:.2f}")
                return result
        
        return None
    
    async def get_option_expirations(self, symbol: str) -> List[date]:
        """Get available expiration dates"""
        
        logger.info(f"Getting expirations for {symbol}")
        endpoint = f"marketdata/options/expirations/{symbol}"
        url = f"{self.base_url}/{endpoint}"
        headers = self.auth.get_headers()
        
        async with self.session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            
            dates = []
            if 'Expirations' in data:
                for exp in data['Expirations']:
                    exp_date = datetime.strptime(exp['Date'], '%Y-%m-%dT%H:%M:%SZ').date()
                    dates.append(exp_date)
                logger.info(f"Found {len(dates)} expirations")
            
            return sorted(dates)
    
    async def stream_options_chain(self, underlying: str, expiration: date, handler: Callable):
        """
        Stream real-time options chain data
        
        Args:
            underlying: Underlying symbol (e.g., 'SPY')
            expiration: Option expiration date
            handler: Async callback function to process updates
        """
        
        exp_str = expiration.strftime('%Y-%m-%d')
        
        endpoint = f"marketdata/stream/options/chains/{underlying}"
        params = {'expiration': exp_str}
        
        url = f"{self.base_url}/{endpoint}"
        headers = self.auth.get_headers()
        
        logger.info(f"Starting options stream for {underlying} exp {exp_str}")
        logger.info(f"Stream URL: {url}")
        
        try:
            async with self.session.get(url, headers=headers, params=params) as response:
                logger.info(f"Response status: {response.status}")
                
                response.raise_for_status()
                
                logger.info(f"✅ Stream connection established (status: {response.status})")
                logger.info(f"Response headers: {dict(response.headers)}")
                self.is_streaming = True
                logger.info(f"is_streaming flag set to True")
                
                buffer = ""
                
                async for chunk in response.content.iter_chunked(8192):
                    try:
                        chunk_text = chunk.decode('utf-8')
                        buffer += chunk_text
                        
                        while '\n' in buffer:
                            line, buffer = buffer.split('\n', 1)
                            line = line.strip()
                            
                            if not line:
                                continue
                            
                            try:
                                data = json.loads(line)
                                
                                self.messages_received += 1
                                self.last_message_time = datetime.now()
                                
                                if self.messages_received <= 3:
                                    logger.info(f"\n{'='*60}")
                                    logger.info(f"Stream Message #{self.messages_received}:")
                                    logger.info(json.dumps(data, indent=2)[:2000])
                                    logger.info(f"{'='*60}\n")
                                elif self.messages_received % 100 == 0:
                                    logger.info(f"📊 Received {self.messages_received} messages")
                                
                                await handler(data)
                                
                            except json.JSONDecodeError as e:
                                logger.warning(f"JSON parse error: {e}")
                                self.errors_count += 1
                        
                    except UnicodeDecodeError as e:
                        logger.error(f"Decode error: {e}")
                        self.errors_count += 1
                    except Exception as e:
                        logger.error(f"Chunk processing error: {e}", exc_info=True)
                        self.errors_count += 1
                
                self.is_streaming = False
                logger.info("Stream ended")
                
        except aiohttp.ClientError as e:
            logger.error(f"Stream connection error: {e}", exc_info=True)
            self.is_streaming = False
            self.errors_count += 1
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            self.is_streaming = False
            self.errors_count += 1
            raise
    
    async def start_streaming_0dte(self, symbol: str, handler: Callable):
        """
        Start streaming today's 0DTE options chain
        
        Args:
            symbol: Underlying symbol
            handler: Async callback to process option updates
        """
        
        logger.info(f"Setting up 0DTE streaming for {symbol}")
        
        quote = await self.get_quote(symbol)
        if not quote:
            raise ValueError(f"Could not get quote for {symbol}")
        
        today = date.today()
        logger.info(f"Looking for 0DTE expiration: {today}")
        
        expirations = await self.get_option_expirations(symbol)
        
        if today not in expirations:
            logger.error(f"No 0DTE options for {symbol} on {today}")
            logger.info(f"Available expirations: {expirations[:5]}")
            raise ValueError("No 0DTE options available")
        
        logger.info(f"✅ Found 0DTE expiration: {today}")
        
        logger.info("🚀 Starting options stream...")
        self.stream_task = asyncio.create_task(
            self.stream_options_chain(symbol, today, handler)
        )
        
        logger.info("Stream task started")
    
    async def stop_streaming(self):
        """Stop streaming"""
        
        logger.info("Stopping stream...")
        self.is_streaming = False
        
        if self.stream_task:
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Stream stopped")
    
    def get_stats(self) -> Dict:
        """Get streaming statistics"""
        return {
            'is_streaming': self.is_streaming,
            'messages_received': self.messages_received,
            'errors_count': self.errors_count,
            'last_message_time': self.last_message_time
        }


# Test script
async def test_streaming():
    """Test the streaming client"""
    
    client_id = os.getenv('TRADESTATION_CLIENT_ID')
    client_secret = os.getenv('TRADESTATION_CLIENT_SECRET')
    refresh_token = os.getenv('TRADESTATION_REFRESH_TOKEN')
    use_sandbox = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    
    logger.info("="*80)
    logger.info("TradeStation Options Streaming Test")
    logger.info("="*80)
    
    message_count = 0
    
    async def stream_handler(data: dict):
        nonlocal message_count
        message_count += 1
    
    async with TradeStationStreamingClient(
        client_id, client_secret, refresh_token, sandbox=use_sandbox
    ) as client:
        
        try:
            await client.start_streaming_0dte('SPY', stream_handler)

            logger.info("\nStreaming for 60 seconds...")
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
        
        stats = client.get_stats()
        logger.info("\n" + "="*80)
        logger.info("Streaming Results:")
        logger.info(f"  Messages received: {stats['messages_received']}")
        logger.info(f"  Handler calls: {message_count}")
        logger.info(f"  Errors: {stats['errors_count']}")
        logger.info("="*80)


if __name__ == '__main__':
    try:
        asyncio.run(test_streaming())
    except KeyboardInterrupt:
        logger.info("\nStopped by user")
