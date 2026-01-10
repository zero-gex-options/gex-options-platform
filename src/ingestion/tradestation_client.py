"""
TradeStation Streaming Client

Handles streaming market data from TradeStation API.
"""

import asyncio
import aiohttp
import json
from datetime import datetime, date
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

    BASE_URL = "https://api.tradestation.com/v3"
    SANDBOX_URL = "https://sim-api.tradestation.com/v3"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize streaming client

        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: Refresh token
            sandbox: Use sandbox environment
        """
        logger.debug(f"Initializing TradeStationStreamingClient...")

        self.auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)
        self.sandbox = sandbox
        self.base_url = self.SANDBOX_URL if sandbox else self.BASE_URL

        if sandbox:
            logger.warning("Using SANDBOX environment [self.base_url] - data may not be real-time")
        else:
            logger.info("Using PRODUCTION environment [self.base_url]")

        self.session = None
        self.is_streaming = False
        self.stream_task = None

        # Stats
        self.messages_received = 0
        self.errors_count = 0
        self.last_message_time = None

        logger.info("TradeStation streaming client initialized")

    async def __aenter__(self):
        """Context manager entry"""
        logger.debug("Entering TradeStationStreamingClient context...")
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=300)
        self.session = aiohttp.ClientSession()
        logger.debug("Created aiohttp session")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        logger.debug("Exiting TradeStationStreamingClient context...")
        logger.info("Stopping stream...")
        self.is_streaming = False

        if self.stream_task and not self.stream_task.done():
            logger.debug("Cancelling stream task...")
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                logger.debug("Stream task cancelled successfully")

        if self.session:
            logger.debug("Closing aiohttp session...")
            await self.session.close()

        logger.debug("TradeStation client closed")
        logger.info("✅ Stream stopped")

    async def get_quote(self, symbol: str) -> Optional[Dict]:
        """
        Get current quote for symbol

        Args:
            symbol: Symbol to quote

        Returns:
            Quote data or None
        """

        url = f"{self.base_url}/marketdata/barcharts/{symbol}"
        logger.info(f"Requesting quote for {symbol}...")

        # Get fresh token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'

        # Set params for API GET
        params = {
            'unit': 'Minute', # Unit of time for each bar interval.
            'barsback': '1',  # Number of bars back to fetch (or retrieve).
            'sessiontemplate': 'USEQ24Hour' # United States (US) stock market session templates.
        }
        logger.debug(f"Attempting to fetch data from {url} with {params}")

        try:
            async with self.session.get(url, headers=headers, params=params, timeout=10) as response:
                logger.debug(f"Quote request status: {response.status}")

                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Quote request failed with status {response.status}: {error_text}")
                    return None

                data = await response.json()
                pretty_data = json.dumps(data, indent=4)
                logger.debug("Full JSON response:")
                logger.debug(pretty_data)

                if 'Bars' not in data or len(data['Bars']) == 0:
                    logger.warning(f"No quote data returned for {symbol}")
                    return None

                quote = data['Bars'][0]
                pretty_quote = json.dumps(quote, indent=4)
                price = float(quote.get('Close', 0))

                logger.info(f"✅ {symbol}: ${price}")
                logger.debug("Full quote data:")
                logger.debug(pretty_quote)

                realtime = False
                if str(quote.get('IsRealtime')).lower() == 'true':
                    realtime = True

                return {
                    'symbol': symbol,
                    'high': quote.get('High', 0),
                    'low': quote.get('Low', 0),
                    'open': quote.get('Open', 0),
                    'close': price,
                    'timestamp': quote.get('TimeStamp', 0),
                    'realtime': realtime,
                    'total_vol': quote.get('TotalVolume', 0),
                    'down_vol': quote.get('DownVolume', 0),
                    'up_vol': quote.get('UpVolume', 0),
                }

        except asyncio.TimeoutError:
            logger.error(f"Quote request timed out for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error getting quote for {symbol}: {e}", exc_info=True)
            return None

    async def get_option_expirations(self, underlying: str) -> List[date]:
        """
        Get available option expiration dates

        Args:
            underlying: Underlying symbol

        Returns:
            List of expiration dates
        """
        logger.debug(f"Requesting option expirations for {underlying}...")

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

                expirations = []
                if 'Expirations' in data:
                    for exp in data['Expirations']:
                        exp_date = datetime.strptime(exp['Date'], '%Y-%m-%dT%H:%M:%SZ').date()
                        expirations.append(exp_date)
                    logger.info(f"✅ Found {len(expirations)} expirations for {underlying}")
                    logger.debug(f"Expirations: {expirations[:5]}..." if len(expirations) > 5 else f"Expirations: {expirations}")

                return sorted(expirations)

        except asyncio.TimeoutError:
            logger.error(f"Expirations request timed out for {underlying}")
            return []
        except Exception as e:
            logger.error(f"Error getting expirations for {underlying}: {e}", exc_info=True)
            return []

    async def stream_options_chain(self, underlying: str, expiration: date, handler: Callable):
        """
        Stream real-time options chain for given underlying and expiration

        Args:
            underlying: Underlying symbol (e.g., 'SPY')
            expiration: Option expiration date
            handler: Async callback function to handle each update
        """

        exp_str = expiration.strftime('%Y-%m-%d')
        stream_url = f"{self.base_url}/marketdata/stream/options/chains/{underlying}"
        logger.info(f"🚀 Starting options stream for {underlying} exp {exp_str}")

        # Get fresh token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'

        # Set params for API GET
        # { 'expiration': expiration string ('YYYY-MM-DD') }
        params = {'expiration': exp_str}
        logger.debug(f"Attempting to establish stream connection to {stream_url} with {params}")

        try:
            async with self.session.get(stream_url, headers=headers, params=params) as response:
                logger.debug(f"Response status: {response.status}")

                if response.status != 200:
                    error_text = await response.text()
                    logger.critical(f"Stream connection failed with status {response.status}: {error_text}")
                    return

                logger.info(f"✅ Stream connection established!")
                logger.debug(f"Response headers: {dict(response.headers)}")
                self.is_streaming = True
                logger.debug(f"is_streaming flag set to True")

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
                                    logger.debug(f"Stream Message #{self.messages_received}:")
                                    logger.debug(json.dumps(data, indent=2)[:2000])
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

    def get_stats(self) -> Dict:
        """Get streaming statistics"""
        return {
            'is_streaming': self.is_streaming,
            'messages_received': self.messages_received,
            'errors_count': self.errors_count,
            'last_message_time': self.last_message_time
        }


# Test
async def main():

    print("\n" + "="*60)
    print("Testing TradeStation streaming client...")
    print("="*60 + "\n\n")

    async with TradeStationStreamingClient(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    ) as client:

        # Test 1: Get quote
        print("\n" + "="*60)
        print("--- Test 1: Getting Quote ---\n")
        quote = await client.get_quote('SPY')
        if quote:
            print(f"✅ Quote test successful: SPY @ ${quote['close']}")
            print(f"   Time: {quote['timestamp']}")
            print(f"     * Realtime" if str({quote['realtime']}).lower() == 'true' else "     * Not realtime")
            print(f"   High: ${quote['high']}")
            print(f"   Low: ${quote['low']}")
            print(f"   Open: ${quote['open']}")
            print(f"   Close: ${quote['close']}")
            print(f"   Volume: {quote['total_vol']}")
            print(f"     ↑: {quote['up_vol']}")
            print(f"     ↓: {quote['down_vol']}")
        else:
            print("❌ Quote test failed")

        # Test 2: Get expirations
        print("\n" + "="*60)
        print("--- Test 2: Getting Expirations ---\n")
        exps = await client.get_option_expirations('SPY')
        if exps:
            print(f"✅ Expirations test successful: {len(exps)} expirations found")

            print("   ", end="")
            for exp in exps[:5]:
                exp_str = exp.strftime('%Y-%m-%d')
                print(f"{exp_str}, ", end="")
            print("...")

            # Find today's expiration (0DTE)
            today = date.today()
            if today in exps:
                print(f"   ✅ 0DTE expiration available: {today}")
                test_expiration = today
            else:
                print(f"   No 0DTE today, using nearest: {exps[0]}")
                test_expiration = exps[0]

            # Test 3: Stream options for 30 seconds
            print("\n" + "="*60)
            print(f"--- Test 3: Streaming Options for {test_expiration} ---\n")
            print("*** will stream for 30 seconds and show sample quotes ***")

            options_received = 0
            sample_quotes = []

            async def test_handler(data: Dict):
                """Handler to collect sample quotes"""
                nonlocal options_received, sample_quotes
                options_received += 1

                # Collect first 5 unique quotes
                if len(sample_quotes) < 5:
                    symbol = data.get('Symbol', '')
                    if symbol and symbol not in [q.get('Symbol') for q in sample_quotes]:
                        sample_quotes.append(data)
                        print(f"   Sample quote #{len(sample_quotes)}: {symbol}")
                        print(f"      Bid: ${data.get('Bid', 0):.2f}, Ask: ${data.get('Ask', 0):.2f}")
                        print(f"      Strike: ${data.get('Strike', 0):.2f}, OI: {data.get('OpenInterest', 0):,}")

                if options_received % 50 == 0:
                    print(f"   Received {options_received} option quotes so far...")

            # Create stream task
            stream_task = asyncio.create_task(
                client.stream_options_chain('SPY', test_expiration, test_handler)
            )

            # Let it run for 10 seconds
            try:
                await asyncio.wait_for(stream_task, timeout=10)
            except asyncio.TimeoutError:
                print("   Stream test timeout reached (30s)")
                stream_task.cancel()
                try:
                    await stream_task
                except asyncio.CancelledError:
                    pass

            print(f"\n✅ Stream test complete!")
            print(f"   Total quotes received: {options_received}")
            print(f"   Sample quotes collected: {len(sample_quotes)}")

            if options_received > 0:
                print("\n📊 Stream Performance:")
                print(f"   Quotes/second: {options_received / 30:.1f}")
                print(f"   Connection: Successful")
                print(f"   Data flow: Active")
            else:
                print("⚠️  No quotes received during test period")
                print("   This may indicate market is closed or no activity")
        else:
            print("❌ Expirations test failed")

    print("\n" + "="*60)
    print("--- Stats ---\n")
    stats = client.get_stats()
    for stat,value in stats.items():
        print(f"{stat}: {value}")

    print("\n" + "="*60)
    print("All tests complete!")
    print("="*60 + "\n")


if __name__ == '__main__':
    asyncio.run(main())
