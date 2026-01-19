"""
TradeStation Simple Client

Handles fetching market data from TradeStation API.
"""

import os
import requests
import json
from datetime import datetime, date, timezone
import os
import argparse
from tradestation_auth import TradeStationAuth
from src.utils import get_logger

# Initialize logger
logger = get_logger(__name__)

class TradeStationSimpleClient:
    """Simple client for fetching market data from TradeStation API"""

    BASE_URL = "https://api.tradestation.com/v3"
    SANDBOX_URL = "https://sim-api.tradestation.com/v3"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize client

        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: Refresh token for obtaining access tokens
            sandbox: Use sandbox environment (default False)
        """
        logger.debug("Initializing TradeStationSimpleClient...")

        self.base_url = self.SANDBOX_URL if sandbox else self.BASE_URL

        self.auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)

        if sandbox:
            logger.warning("Using SANDBOX environment [self.base_url] - data may not be real-time")
        else:
            logger.info("Using PRODUCTION environment [self.base_url]")

        logger.info(f"TradeStationSimpleClient initialized for {'sandbox' if sandbox else 'production'}")

    def _fetch_tradestation_data(self, api_endpoint: str, params: dict = None) -> dict:
        """
        GET call to TradeStation API to fetch data

        Args:
            api_endpoint: i.e. /marketdata/barcharts/{symbol}
            params: JSON parameters to pass to the API call

        Returns:
            Raw JSON response returned by the API
        """

        # Construct full URL for API call
        url = f"{self.base_url}/{api_endpoint}"
        logger.debug(f"Making API GET call to TradeStation {url} with params {params}...")

        # Get fresh access token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'

        try:

            # Make API GET call to https://api.tradestation.com/v3/{endpoint}
            # or for sandbox: https://sim-api.tradestation.com/v3/{endpoint})
            response = requests.get(url, headers=headers, params=params, timeout=10)

            logger.debug(f"API GET call status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"API GET call failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                response.raise_for_status()

            # Parse JSON response and dump the full
            # response to debug logs
            data = response.json()
            pretty_data = json.dumps(data, indent=4)
            logger.debug("Full JSON response:")
            logger.debug(pretty_data)

            return data

        except requests.exceptions.Timeout:
            logger.error("API GET call timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API GET call failed: {e}")
            raise
        except Exception as e:
            logger.critical(f"Error fetching data for {symbol}: {e}", exc_info=True)
            raise

    def get_quote(self,
                        symbol: str = "SPY",
                        unit: str = "Minute",
                        bars_back: str = "1",
                        last_date: str = None,
                        mkt_session: str = None) -> dict:
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

        endpoint = f"marketdata/barcharts/{symbol}"
        logger.info(f"Requesting quote for {symbol}...")

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

        mkt_data = self._fetch_tradestation_data(endpoint, params)

        if 'Bars' not in mkt_data or len(mkt_data['Bars']) == 0:
            logger.warning(f"No quote data returned for {symbol}")
            return None

        quote = mkt_data['Bars'][-1]
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

    def get_option_expirations(self,
                                     underlying: str = "SPY",
                                     strike: str = None) -> list:
        """
        Get available option expiration dates

        Args:
            underlying: Underlying symbol
            strike: Option strike price

        Returns:
            List of expiration dates
        """

        endpoint = f"marketdata/options/expirations/{underlying}"
        logger.info(f"Requesting option expirations for {underlying}...")

        # Set params for API GET
        params = {}
        if strike:
            params['strikePrice']: strike

        mkt_data = self._fetch_tradestation_data(endpoint, params)

        expirations = []
        if 'Expirations' in mkt_data:
            for exp in mkt_data['Expirations']:
                exp_date = datetime.strptime(exp['Date'], '%Y-%m-%dT%H:%M:%SZ').date()
                expirations.append(exp_date)
            logger.info(f"✅ Found {len(expirations)} expirations for {underlying}")
            logger.debug(f"Expirations: {expirations[:5]}..." if len(expirations) > 5 else f"Expirations: {expirations}")

        return sorted(expirations)

    def get_option_strikes (self,
                                  underlying: str = "SPY",
                                  expiration: date = None) -> list:
        """
        Get available option strikes

        Args:
            underlying: Underlying symbol
            expiration: Option expiration date

        Returns:
            List of strikes
        """

        endpoint = f"marketdata/options/strikes/{underlying}"
        logger.info(f"Requesting option strikes for {underlying}...")

        # Set params for API GET
        params = {}
        if expiration:
            params['expiration']: expiration

        mkt_data = self._fetch_tradestation_data(endpoint, params)

        strikes = []
        if 'Strikes' in mkt_data:
            for strike in mkt_data['Strikes']:
                strike_price = strike[0]
                strikes.append(strike_price)
            logger.info(f"✅ Found {len(strikes)} strikes for {underlying}")
            logger.debug(f"Strikes: {strikes[:5]}..." if len(strikes) > 5 else f"Strikes: {strikes}")

        return strikes

def parse_arguments():
    """
    Parse command-line arguments for TradeStation simple client operations.

    Returns:
        argparse.Namespace: Parsed arguments with normalized values
    """
    parser = argparse.ArgumentParser(
        prog='tradestation_client.py',
        description='TradeStation Simple API Client - Query quotes and options data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s --quote
  %(prog)s --quote --symbol AAPL --unit daily --bars-back 10
  %(prog)s --quote --symbol SPY --last-date 2024-01-15T10:00:00Z --mkt-session USEQ24Hour
  %(prog)s --option-expirations --underlying SPY
  %(prog)s --option-expirations --underlying SPY --strike 450
  %(prog)s --option-strikes --underlying AAPL --expiration 2024-12-20

For more help, use -h or --help
        '''
    )

    # Create mutually exclusive group for main operations
    group = parser.add_mutually_exclusive_group()

    # Main command flags
    group.add_argument('--quote', action='store_true', help='Get quote data')
    group.add_argument('--option-expirations', action='store_true', help='Get option expirations')
    group.add_argument('--option-strikes', action='store_true', help='Get option strikes')
    
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
    
    args = parser.parse_args()

    # Default to --quote if no command specified
    if not any([args.quote, args.option_expirations, args.option_strikes]):
        args.quote = True

    # Build parameter dicts based on command
    if args.quote:
        args.params = build_quote_params(args)
    elif args.option_expirations:
        args.params = build_option_expirations_params(args)
    elif args.option_strikes:
        args.params = build_option_strikes_params(args)

    return args

def build_quote_params(args):
    """Build parameters for quote command with defaults."""
    return {
        'symbol': (args.symbol or "SPY").upper(),
        'unit': (args.unit or "Minute").lower().capitalize(),
        'bars_back': args.bars_back or 1,
        'last_date': args.last_date,
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

def main():

    print("\n" + "="*60)
    print("TradeStation Simple API Client...")
    print("="*60 + "\n")

    args = parse_arguments()

    client = TradeStationSimpleClient(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    )

    try:

        #######################################################################
        # Quote
        #######################################################################
        if args.quote:
            logger.info(f"Executing quote command: {args.params}")
            quote = client.get_quote(**args.params)
            now_utc = datetime.now(timezone.utc)
            formatted_time = now_utc.strftime('%FT%TZ')
            print("\n" + "="*60 + "\n")
            if quote:
                print(f"✅ Quote fetched successfully for {args.params['symbol']}:")
                print(f"   Time: {quote['timestamp']} (current time: {formatted_time})")
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
            exps = client.get_option_expirations(**args.params)
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
            strikes = client.get_option_strikes(**args.params)
            print("\n" + "="*60 + "\n")
            if strikes:
                print(f"✅ Strikes fetched successfully for {args.params['underlying']} ({len(strikes)} strikes found):")
                for strike in strikes:
                    print("   $" + strike)
            else:
                print("❌ Strikes fetch failed")
            print("\n" + "="*60 + "\n")

    except Exception as e:
        print(f"❌ Client failed: {e}")


if __name__ == '__main__':
    main()
