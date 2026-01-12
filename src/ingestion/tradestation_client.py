"""
TradeStation Streaming Client

Handles streaming market data from TradeStation API.
"""

import asyncio
import aiohttp
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Callable, Any
import os
import argparse
from tradestation_auth import TradeStationAuth
from src.utils import get_logger

# Initialize logger
logger = get_logger(__name__)

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


    async def fetch_tradestation_data(self, api_endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """
        GET call to TradeStation API to fetch data

        Args:
            api_endpoint: i.e. /marketdata/barcharts/{symbol}
            params: JSON parameters to pass to the API call

        Returns:
            Raw JSON response returned by the API
        """

        # Construct full URL
        url = f"{self.base_url}{api_endpoint}"
        logger.debug(f"Making API GET call to TradeStation {url} with params {params}...")

        # Get fresh token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'

        # Make API GET call and handle response
        try:
            async with self.session.get(url, headers=headers, params=params, timeout=10) as response:

                logger.debug(f"API GET call status: {response.status}")

                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"API GET call failed with status {response.status}: {error_text}")
                    return None

                data = await response.json()
                pretty_data = json.dumps(data, indent=4)
                logger.debug("Full JSON response:")
                logger.debug(pretty_data)

                return data

        except asyncio.TimeoutError:
            logger.error(f"Quote request timed out for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error getting quote for {symbol}: {e}", exc_info=True)
            return None

    async def get_quote(self,
                        symbol: str = "SPY",
                        unit: Optional[str] = "Minute",
                        bars_back: Optional[str] = "1",
                        last_date: Optional[str] = None,
                        mkt_session: Optional[str] = None) -> Optional[Dict]:
        """
        Get current quote for symbol

        Args:
            symbol: Symbol to quote
            unit: Minute, Daily, Weekly, Monthly
            bars_back: number of units to get quotes for
            last_date (optional): uses current timestamp if not specified
            mkt_session (optional): USEQPre, USEQPost, USEQPreAndPost, USEQ24Hour, Default

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
            'unit': unit, # Unit of time for each bar interval.
            'barsback': bars_back,  # Number of bars back to fetch (or retrieve).
            'sessiontemplate': 'USEQ24Hour' # United States (US) stock market session templates.
        }

        # Add lastdate and sessiontemplate
        # if specified
        if last_date:
            params['lastdate'] = last_date
        if mkt_session:
            params['sessiontemplate'] = mkt_session

        logger.debug(f"Attempting to fetch data from {url} with {params}...")

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

                quote = data['Bars'][-1]
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

    async def get_option_expirations(self,
                                     underlying: str = "SPY",
                                     strike: Optional[str] = None) -> Optional[List]:
        """
        Get available option expiration dates

        Args:
            underlying: Underlying symbol
            strike: Option strike price

        Returns:
            List of expiration dates
        """

        url = f"{self.base_url}/marketdata/options/expirations/{underlying}"
        logger.info(f"Requesting option expirations for {underlying}...")

        # Get fresh token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'

        # Set params for API GET
        params = {}
        if strike:
            params['strikePrice']: strike

        logger.debug(f"Attempting to fetch data from {url} with {params}...")

        try:
            async with self.session.get(url, headers=headers, params=params, timeout=10) as response:
                logger.debug(f"Expirations request status: {response.status}")

                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Expirations request failed: {error_text}")
                    return []

                data = await response.json()
                pretty_data = json.dumps(data, indent=4)
                logger.debug("Full JSON response:")
                logger.debug(pretty_data)

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

    async def get_option_strikes (self,
                                  underlying: str = "SPY",
                                  expiration: Optional[date] = None) -> Optional[List]:
        """
        Get available option strikes

        Args:
            underlying: Underlying symbol
            expiration: Option expiration date

        Returns:
            List of strikes
        """

        url = f"{self.base_url}/marketdata/options/strikes/{underlying}"
        logger.info(f"Requesting option strikes for {underlying}...")

        # Get fresh token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'

        # Set params for API GET
        params = {}
        if expiration:
            params['expiration']: expiration

        logger.debug(f"Attempting to fetch data from {url} with {params}...")

        try:
            async with self.session.get(url, headers=headers, params=params, timeout=10) as response:
                logger.debug(f"Strikes request status: {response.status}")

                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Strikes request failed: {error_text}")
                    return []

                data = await response.json()
                pretty_data = json.dumps(data, indent=4)
                logger.debug("Full JSON response:")
                logger.debug(pretty_data)

                strikes = []
                if 'Strikes' in data:
                    for strike in data['Strikes']:
                        strike_price = strike[0]
                        strikes.append(strike_price)
                    logger.info(f"✅ Found {len(strikes)} strikes for {underlying}")
                    logger.debug(f"Strikes: {strikes[:5]}..." if len(strikes) > 5 else f"Strikes: {strikes}")

                return strikes

        except asyncio.TimeoutError:
            logger.error(f"Strikes request timed out for {underlying}")
            return []
        except Exception as e:
            logger.error(f"Error getting strikes for {underlying}: {e}", exc_info=True)
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


def parse_arguments():
    """
    Parse command-line arguments for TradeStation client operations.

    Returns:
        argparse.Namespace: Parsed arguments with normalized values
    """
    parser = argparse.ArgumentParser(
        prog='tradestation_client.py',
        description='TradeStation API Client - Query quotes, options data, and stream market data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s --quote
  %(prog)s --quote --symbol AAPL --unit daily --bars-back 10
  %(prog)s --quote --symbol SPY --last-date 2024-01-15T10:00:00Z --mkt-session USEQ24Hour
  %(prog)s --option-expirations --underlying SPY
  %(prog)s --option-expirations --underlying SPY --strike 450
  %(prog)s --option-strikes --underlying AAPL --expiration 2024-12-20
  %(prog)s --stream-options --underlying SPY --proximity 10 --range ITM --type Call
  %(prog)s --stream-options --expiration 2024-12-20 --type Put

For more help, use -h or --help
        '''
    )

    # Create mutually exclusive group for main operations
    group = parser.add_mutually_exclusive_group()

    # Main command flags
    group.add_argument('--quote', action='store_true', help='Get quote data')
    group.add_argument('--option-expirations', action='store_true', help='Get option expirations')
    group.add_argument('--option-strikes', action='store_true', help='Get option strikes')
    group.add_argument('--stream-options', action='store_true', help='Stream options data')
    
    # Quote arguments
    parser.add_argument('--symbol', type=str, help='Stock symbol (default: SPY)')
    parser.add_argument('--unit', type=str, help='Time unit: minute, daily, etc. (default: minute)')
    parser.add_argument('--bars-back', type=int, help='Number of bars to retrieve (default: 1)')
    parser.add_argument('--last-date', type=str, help='End date in ISO format, e.g. 2026-01-01 or 2026-01-01T00:00:00Z (default: current)')
    parser.add_argument('--mkt-session', type=str, help='US stock market session templates (default: Default)')
    
    # Option arguments (shared across option commands)
    parser.add_argument('--underlying', type=str, help='Underlying symbol (default: SPY)')
    parser.add_argument('--strike', type=float, help='Strike price filter (optional)')
    parser.add_argument('--expiration', type=str, help='Expiration date (optional)')
    
    # Stream-specific arguments
    parser.add_argument('--proximity', type=int, help='Strike proximity (default: 5)')
    parser.add_argument('--range', type=str, choices=['ALL', 'ITM', 'OTM'],
                       help='Option range: All, ITM, or OTM (default: All)')
    parser.add_argument('--type', type=str, choices=['All', 'Call', 'Put'],
                       help='Option type: All, Call, or Put (default: All)')

    args = parser.parse_args()

    # Default to --quote if no command specified
    if not any([args.quote, args.option_expirations, args.option_strikes, args.stream_options]):
        args.quote = True

    # Build parameter dicts based on command
    if args.quote:
        args.params = build_quote_params(args)
    elif args.option_expirations:
        args.params = build_option_expirations_params(args)
    elif args.option_strikes:
        args.params = build_option_strikes_params(args)
    elif args.stream_options:
        args.params = build_stream_options_params(args)

    return args

def build_quote_params(args):
    """Build parameters for quote command with defaults."""
    return {
        'symbol': (args.symbol or 'SPY').upper(),
        'unit': (args.unit or 'Minute').lower(),
        'bars_back': args.bars_back or 1,
        'last_date': args.last_date or datetime.now().isoformat(),
        'mkt_session': args.mkt_session
    }


def build_option_expirations_params(args):
    """Build parameters for option-expirations command with defaults."""
    return {
        'underlying': (args.underlying or 'SPY').upper(),
        'strike': args.strike
    }


def build_option_strikes_params(args):
    """Build parameters for option-strikes command with defaults."""
    return {
        'underlying': (args.underlying or 'SPY').upper(),
        'expiration': args.expiration
    }


def build_stream_options_params(args):
    """Build parameters for stream-options command with defaults."""
    # Normalize range value
    range_value = 'ALL'
    if args.range:
        range_value = args.range.upper()
    
    # Normalize type value
    type_value = 'All'
    if args.type:
        type_value = args.type.capitalize()
    
    return {
        'underlying': (args.underlying or 'SPY').upper(),
        'expiration': args.expiration,
        'proximity': args.proximity or 5,
        'range': range_value,
        'type': type_value
    }

async def main():

    print("\n" + "="*60)
    print("TradeStation API Client...")
    print("="*60 + "\n")

    args = parse_arguments()

    async with TradeStationStreamingClient(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    ) as client:

        #######################################################################
        # Quote
        #######################################################################
        if args.quote:
            logger.info(f"Executing quote command: {args.params}")
            quote = await client.get_quote(**args.params)
            print("\n" + "="*60 + "\n")
            if quote:
                print(f"✅ Quote fetched successfully for {args.params['symbol']}:")
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
                print("❌ Quote fetch failed")
            print("\n" + "="*60 + "\n")

        #######################################################################
        # Option Expirations
        #######################################################################
        elif args.option_expirations:
            logger.info(f"Executing option-expirations command: {args.params}")
            exps = await client.get_option_expirations(**args.params)
            print("\n" + "="*60 + "\n")
            if exps:
                print(f"✅ Expirations fetched successfully for {args.params['underlying']} ({len(exps)} expirations found):")
                for exp in exps:
                    print("   " + exp.strftime('%Y-%m-%d'))
            else:
                print("❌ Expirations fetch failed")
            print("\n" + "="*60 + "\n")

        #######################################################################
        # Option Strikes
        #######################################################################
        elif args.option_strikes:
            logger.info(f"Executing option-strikes command: {args.params}")
            strikes = await client.get_option_strikes(**args.params)
            print("\n" + "="*60 + "\n")
            if strikes:
                print(f"✅ Strikes fetched successfully for {args.params['underlying']} ({len(strikes)} strikes found):")
                for strike in strikes:
                    print("   $" + strike)
            else:
                print("❌ Strikes fetch failed")
            print("\n" + "="*60 + "\n")


        #######################################################################
        # Options Stream
        #######################################################################
        elif args.stream_options is not None:
            logger.info(f"Executing stream-options command: {args.params}")

    print("\n" + "="*60 + "\n")
    print("--- Stats ---")
    stats = client.get_stats()
    for stat,value in stats.items():
        print(f"{stat}: {value}")
    print()

if __name__ == '__main__':
    asyncio.run(main())
