"""
TradeStation Streaming Ingestion Engine

Real-time ingestion of SPY 0DTE options data with batch processing.
"""

import asyncio
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, time as dt_time, date, timezone
import pytz
from typing import Dict, Optional
import logging
import os
import sys
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from tradestation_client import TradeStationStreamingClient
from greeks_calculator import GreeksCalculator

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


class StreamingIngestionEngine:
    """Ingest real-time options data from TradeStation"""

    def __init__(self):
        logger.info("🔄 Initializing Streaming Ingestion Engine...")
        logger.debug(f"Batch size: {os.getenv('BATCH_SIZE', 100)}")

        # Database connection
        logger.debug("Connecting to database...")
        try:
            self.db_conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                port=os.getenv('DB_PORT')
            )
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.critical(f"Failed to connect to database: {e}", exc_info=True)
            raise

        # TradeStation credentials
        self.ts_client_id = os.getenv('TRADESTATION_CLIENT_ID')
        self.ts_client_secret = os.getenv('TRADESTATION_CLIENT_SECRET')
        self.ts_refresh_token = os.getenv('TRADESTATION_REFRESH_TOKEN')
        self.ts_sandbox = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'

        if not all([self.ts_client_id, self.ts_client_secret, self.ts_refresh_token]):
            logger.critical("Missing TradeStation credentials in environment variables")
            raise ValueError("Missing required TradeStation credentials")

        logger.debug("TradeStation credentials loaded")

        # Batch processing
        self.batch_size = int(os.getenv('BATCH_SIZE', 100))
        self.batch_buffer = []
        self.batch_lock = asyncio.Lock()

        # Greeks validation
        self.validate_greeks = os.getenv('VALIDATE_GREEKS', 'false').lower() == 'true'
        self.greeks_calc = GreeksCalculator() if self.validate_greeks else None

        # Validation tolerances (percentage)
        # Note: 0DTE options have larger expected mismatches due to:
        # - American vs European option differences
        # - Model differences (Black-Scholes vs proprietary)
        # - Time-to-expiration precision
        # - Dividend timing
        self.delta_tolerance = 0.10  # 10%
        self.gamma_tolerance = 0.25  # 25%
        self.vega_tolerance = 0.20   # 20%
        self.theta_tolerance = 0.25  # 25%

        # Statistics
        self.options_received = 0
        self.options_stored = 0
        self.batch_count = 0
        self.error_count = 0
        self.heartbeat_count = 0
        self.last_heartbeat = None

        # Validation stats
        self.greeks_validated = 0
        self.greeks_mismatches = {
            'delta': 0,
            'gamma': 0,
            'theta': 0,
            'vega': 0
        }

        # Track underlying price
        self.underlying_price = None

        logger.info("✅ Streaming Ingestion Engine initialized")
        logger.debug(f"   Batch size: {self.batch_size}")
        logger.debug(f"   Sandbox mode: {self.ts_sandbox}")
        logger.debug(f"   Greeks validation: {'ENABLED' if self.validate_greeks else 'DISABLED'}")
        if self.validate_greeks:
            logger.debug(f"   Delta tolerance: {self.delta_tolerance}")
            logger.debug(f"   Gamma tolerance: {self.gamma_tolerance}")
            logger.debug(f"   Vega tolerance: {self.vega_tolerance}")
            logger.debug(f"   Theta tolerance: {self.theta_tolerance}")

    def is_market_open(self) -> bool:
        """Check if market is currently open"""
        now = datetime.now(pytz.timezone('America/New_York'))

        logger.debug(f"Market hours check: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.debug(f"Day: {now.strftime('%A')} (weekday: {now.weekday()})")

        # Check if weekend
        if now.weekday() >= 5:
            logger.debug("Market closed: Weekend")
            return False

        # Market hours: 9:30 AM to 4:15 PM ET
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        current_time = now.time()

        is_open = market_open <= current_time <= market_close

        logger.debug(f"Market hours: {market_open} - {market_close}, Current: {current_time}, Open: {is_open}")

        return is_open

    async def option_update_handler(self, data: Dict):
        """Handle incoming option update from stream"""

        self.options_received += 1

        if self.options_received % 50 == 0:
            logger.debug(f"Options received: {self.options_received}, Stored: {self.options_stored}")
        else:
            logger.debug(f"Option update #{self.options_received}")

        try:
            # Parse the option data
            option = self._parse_option_update(data)

            # If data is not a valid option
            # check to see if it's just a heartbeat
            if not option:
                if 'Heartbeat' in data:
                    self._handle_heartbeat(data)
                else:
                    logger.warning("Failed to parse option data")
                    logger.debug(f"Invalid data: {data}")
                return

            # Validate Greeks if enabled
            if self.validate_greeks and option['implied_vol'] > 0:
                self._validate_greeks(option)

            # Add to batch buffer
            async with self.batch_lock:
                self.batch_buffer.append(option)
                buffer_size = len(self.batch_buffer)

                logger.debug(f"Added to batch buffer (size: {buffer_size}/{self.batch_size})")

                # Flush if batch is full
                if buffer_size >= self.batch_size:
                    logger.info(f"Batch full ({buffer_size} records), flushing...")
                    await self._flush_batch()

        except Exception as e:
            self.error_count += 1
            logger.error(f"Error handling option update: {e}", exc_info=True)
            if self.error_count % 10 == 0:
                logger.warning(f"Error count reached {self.error_count}")

    def _parse_option_update(self, data: Dict) -> Optional[Dict]:
        """Parse raw option data from stream"""

        logger.debug("Parsing option update...")

        try:

            # Extract leg info
            if not data.get('Legs') or len(data['Legs']) == 0:
                return None

            leg = data['Legs'][0]

            # Parse symbol
            symbol = leg.get('Symbol')
            if not symbol:
                return None

            # Parse underlying
            parts = symbol.split()
            if len(parts) < 2:
                return None

            underlying = parts[0]

            # Parse expiration
            exp_str = leg.get('Expiration')
            expiration = datetime.fromisoformat(exp_str.replace('Z', '+00:00')).date()

            # Parse strike
            strike = float(leg.get('StrikePrice', 0))

            # Parse option type
            option_type = leg.get('OptionType', '').lower()

            # Calculate DTE, OI and IV
            dte = (expiration - date.today()).days
            open_interest = int(data.get('DailyOpenInterest', 0))
            implied_vol = float(data.get('ImpliedVolatility', 0))

            # Build option dict with TradeStation Greeks
            option = {
                'symbol': symbol,
                'underlying': underlying,
                'strike': strike,
                'expiration': expiration,
                'option_type': option_type,
                'bid': float(data.get('Bid', 0)),
                'ask': float(data.get('Ask', 0)),
                'mid': float(data.get('Mid', 0)),
                'last': float(data.get('Last', 0)),
                'volume': int(data.get('Volume', 0)),
                'open_interest': open_interest,
                'implied_vol': implied_vol,
                'delta': float(data.get('Delta', 0)),
                'gamma': float(data.get('Gamma', 0)),
                'theta': float(data.get('Theta', 0)),
                'vega': float(data.get('Vega', 0)),
                'rho': float(data.get('Rho', 0)),
                'timestamp': datetime.now(timezone.utc),
                'underlying_price': self.underlying_price,
                'dte': dte,
                'is_calculated': False
            }

            # Calculate spread percentage
            if option['bid'] > 0 and option['ask'] > 0:
                option['spread_pct'] = (option['ask'] - option['bid']) / option['mid'] if option['mid'] > 0 else 0
            else:
                option['spread_pct'] = None

            # Log results
            logger.info(f"✅ Parsed: {underlying} {strike}{option_type[0].upper()} (DTE={dte}, IV={implied_vol:.2f})")
            option_dump = ', '.join(f"{key}: {value}" for key, value in option.items())
            logger.debug(option_dump)

            return option

        except KeyError as e:
            logger.error(f"Missing key in option data: {e}")
            logger.debug(f"Data keys: {data.keys()}")
            return None
        except Exception as e:
            logger.error(f"Error parsing option data: {e}", exc_info=True)
            return None

    def _handle_heartbeat(self, data: Dict):
        """Handle heartbeat returned during stream"""

        self.heartbeat_count = data['Heartbeat']
        self.last_heartbeat = datetime.fromisoformat(data['Timestamp'].replace('Z', '+00:00'))
        timestamp_str = self.last_heartbeat.strftime('%Y-%m-%d %H:%M:%S %Z')
        logger.debug(f"Received heartbeat 💓 #{self.heartbeat_count} at {timestamp_str}")

    def _validate_greeks(self, option: Dict):
        """Validate TradeStation Greeks against calculated Greeks"""

        try:
            # Calculate Greeks using Black-Scholes with dividends
            calculated = self.greeks_calc.calculate_greeks(
                underlying_price=option['underlying_price'],
                strike=option['strike'],
                expiration=option['expiration'],
                option_type=option['option_type'],
                implied_vol=option['implied_vol'],
                current_time=option['timestamp']
            )

            self.greeks_validated += 1

            # Compare each Greek
            ts_delta = option['delta']
            ts_gamma = option['gamma']
            ts_theta = option['theta']
            ts_vega = option['vega']

            calc_delta = calculated['delta']
            calc_gamma = calculated['gamma']
            calc_theta = calculated['theta']
            calc_vega = calculated['vega']

            # Calculate percentage differences
            def pct_diff(ts_val, calc_val):
                if abs(calc_val) < 1e-6:  # Avoid division by very small numbers
                    return 0 if abs(ts_val) < 1e-6 else 999  # Both ~zero = match, otherwise big diff
                return abs(ts_val - calc_val) / abs(calc_val)

            delta_diff = pct_diff(ts_delta, calc_delta)
            gamma_diff = pct_diff(ts_gamma, calc_gamma)
            theta_diff = pct_diff(ts_theta, calc_theta)
            vega_diff = pct_diff(ts_vega, calc_vega)

            # For 0DTE, only log significant mismatches
            # Skip validation for deep OTM options (delta < 0.05) as they're less reliable
            if abs(ts_delta) < 0.05:
                return

            # Check tolerances and log mismatches
            mismatches = []

            if delta_diff > self.delta_tolerance:
                self.greeks_mismatches['delta'] += 1
                mismatches.append(f"Delta: TS={ts_delta:.4f} Calc={calc_delta:.4f} ({delta_diff*100:.1f}%)")

            if gamma_diff > self.gamma_tolerance:
                self.greeks_mismatches['gamma'] += 1
                mismatches.append(f"Gamma: TS={ts_gamma:.6f} Calc={calc_gamma:.6f} ({gamma_diff*100:.1f}%)")

            if theta_diff > self.theta_tolerance:
                self.greeks_mismatches['theta'] += 1
                mismatches.append(f"Theta: TS={ts_theta:.4f} Calc={calc_theta:.4f} ({theta_diff*100:.1f}%)")

            if vega_diff > self.vega_tolerance:
                self.greeks_mismatches['vega'] += 1
                mismatches.append(f"Vega: TS={ts_vega:.4f} Calc={calc_vega:.4f} ({vega_diff*100:.1f}%)")

            # Only log if multiple Greeks mismatch (more likely a real issue)
            if len(mismatches) >= 2:
                logger.warning(f"Multiple Greeks mismatches for {option['symbol']} (${option['strike']:.2f} {option['option_type'].upper()}):")
                for mismatch in mismatches:
                    logger.warning(f"  {mismatch}")

            # Log validation summary less frequently for 0DTE
            if self.greeks_validated % 500 == 0:  # Every 500 instead of 100
                self._log_validation_summary()

        except Exception as e:
            logger.error(f"Greeks validation error: {e}")

    def _log_validation_summary(self):
        """Log summary of Greeks validation"""

        total_mismatches = sum(self.greeks_mismatches.values())
        mismatch_rate = (total_mismatches / self.greeks_validated * 100) if self.greeks_validated > 0 else 0

        logger.info(f"\n{'='*60}")
        logger.info(f"GREEKS VALIDATION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Options validated: {self.greeks_validated}")
        logger.info(f"Total mismatches: {total_mismatches} ({mismatch_rate:.1f}%)")
        logger.info(f"Breakdown:")
        logger.info(f"  Delta: {self.greeks_mismatches['delta']}")
        logger.info(f"  Gamma: {self.greeks_mismatches['gamma']}")
        logger.info(f"  Theta: {self.greeks_mismatches['theta']}")
        logger.info(f"  Vega: {self.greeks_mismatches['vega']}")
        logger.info(f"{'='*60}\n")

    async def _flush_batch(self):
        """Flush batch buffer to database"""

        if not self.batch_buffer:
            logger.debug("Batch buffer empty, nothing to flush")
            return

        batch_size = len(self.batch_buffer)
        logger.debug(f"Flushing batch of {batch_size} options to database...")

        try:
            self._store_options_batch(self.batch_buffer)

            self.options_stored += batch_size
            self.batch_count += 1
            self.batch_buffer.clear()

            logger.info(f"✅ Batch #{self.batch_count} stored successfully ({self.options_stored} total options)")

            if self.batch_count % 10 == 0:
                logger.debug(f"📊 Stats - Received: {self.options_received}, Stored: {self.options_stored}, "
                           f"Batches: {self.batch_count}, Errors: {self.error_count}")

        except Exception as e:
            logger.error(f"Failed to flush batch: {e}", exc_info=True)
            self.error_count += 1
            raise

    def _store_options_batch(self, batch: list):
        """Store batch of options to database"""

        logger.debug(f"Preparing to insert {len(batch)} records")

        cursor = self.db_conn.cursor()

        values = []
        for opt in batch:
            values.append((
                opt['timestamp'],
                opt['symbol'],
                opt['underlying_price'],
                opt['strike'],
                opt['expiration'],
                opt['dte'],
                opt['option_type'],
                opt['bid'],
                opt['ask'],
                opt['mid'],
                opt['last'],
                opt['volume'],
                opt['open_interest'],
                opt['implied_vol'],
                opt['delta'],
                opt['gamma'],
                opt['theta'],
                opt['vega'],
                opt['rho'],
                opt['is_calculated'],
                opt['spread_pct'],
                'tradestation_stream'
            ))

        insert_query = """
            INSERT INTO options_quotes 
            (timestamp, symbol, underlying_price, strike, expiration, dte,
             option_type, bid, ask, mid, last, volume, open_interest,
             implied_vol, delta, gamma, theta, vega, rho,
             is_calculated, spread_pct, source)
            VALUES %s
            ON CONFLICT (timestamp, symbol, strike, expiration, option_type) 
            DO UPDATE SET
                bid = EXCLUDED.bid,
                ask = EXCLUDED.ask,
                mid = EXCLUDED.mid,
                last = EXCLUDED.last,
                volume = EXCLUDED.volume,
                open_interest = EXCLUDED.open_interest,
                implied_vol = EXCLUDED.implied_vol,
                delta = EXCLUDED.delta,
                gamma = EXCLUDED.gamma,
                theta = EXCLUDED.theta,
                vega = EXCLUDED.vega
        """

        try:
            execute_values(cursor, insert_query, values)
            self.db_conn.commit()
            logger.debug("Database commit successful")
        except Exception as e:
            logger.error(f"❌ Database error: {e}", exc_info=True)
            self.db_conn.rollback()
            raise
        finally:
            cursor.close()

    def _store_underlying_price(self, symbol: str, price: float, volume: int):
        """Store underlying price"""

        logger.debug(f"Storing underlying price: {symbol} = ${price:.2f}")
        self.underlying_price = price

        cursor = self.db_conn.cursor()

        insert_query = """
            INSERT INTO underlying_prices (timestamp, symbol, price, volume, source)
            VALUES (%s, %s, %s, %s, %s)
       """

        try:
            cursor.execute(insert_query, (
                datetime.now(timezone.utc),
                symbol,
                price,
                volume,
                'tradestation_api'
            ))
            self.db_conn.commit()
            logger.debug("Underlying price stored")
        except Exception as e:
            logger.error(f"Error storing underlying price: {e}")
            self.db_conn.rollback()
        finally:
            cursor.close()

    async def run(self, symbol: str = 'SPY'):
        """Main ingestion loop"""

        logger.info("="*60)
        logger.info(f"Starting Streaming Ingestion Engine for {symbol}...")
        if self.validate_greeks:
            logger.info("Greeks validation: ENABLED")
        logger.info("="*60)

        while True:
            try:

                # Only run if market is open unless explicitly
                # specified otherwise
                if (os.getenv('VALIDATE_MARKET_OPEN', 'true').lower() == 'true'):
                    if not self.is_market_open():
                        logger.info("Market closed, waiting 5 minutes...")
                        await asyncio.sleep(300)
                        continue

                    logger.info("Market is open, starting stream...")

                async with TradeStationStreamingClient(
                    self.ts_client_id,
                    self.ts_client_secret,
                    self.ts_refresh_token,
                    sandbox=self.ts_sandbox
                ) as client:

                    # Get quote for underlying symbol
                    logger.debug(f"Getting quote for {symbol}...")
                    quote = await client.get_quote(symbol)

                    if not quote:
                        logger.error(f"Failed to get quote for {symbol}")
                        await asyncio.sleep(60)
                        continue

                    logger.info(f"✅ {symbol}: ${quote['price']:.2f}")

                    # Store underlying price
                    self._store_underlying_price(symbol, quote['price'], quote['volume'])

                    # Get expirations
                    logger.info(f"Getting expirations for {symbol}...")
                    expirations = await client.get_option_expirations(symbol)

                    if not expirations:
                        logger.error(f"No expirations found for {symbol}")
                        await asyncio.sleep(60)
                        continue

                    # Find 0DTE
                    today = date.today()
                    today_str = today.strftime('%Y-%m-%d')

                    if today in expirations:
                        logger.info(f"✅ Found 0DTE expiration: {today_str}")
                        target_expiration = today
                    else:
                        logger.warning(f"No 0DTE expiration today, using nearest: {expirations[0]}")
                        target_expiration = expirations[0]

                    # Start streaming
                    logger.info(f"🚀 Starting options stream for {symbol} {target_expiration}")

                    await client.stream_options_chain(
                        symbol,
                        target_expiration,
                        self.option_update_handler
                    )

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                logger.info("Restarting in 30 seconds...")
                await asyncio.sleep(30)

        # Cleanup
        logger.info("Flushing remaining batch data...")
        await self._flush_batch()

        self.db_conn.close()
        logger.info("Ingestion engine stopped")
        logger.info(f"Final stats - Received: {self.options_received}, Stored: {self.options_stored}, "
                   f"Batches: {self.batch_count}, Errors: {self.error_count}")


async def main():
    engine = StreamingIngestionEngine()
    await engine.run('SPY')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
