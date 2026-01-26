"""
Streaming Options Data Ingestion Engine

Ingests real-time options data from TradeStation using streaming API,
calculates Greeks, and stores in database.
"""

import asyncio
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date, timezone
import time
import os
import sys
import yaml
from pathlib import Path
from typing import Dict, List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from tradestation_streaming_client import TradeStationStreamingClient
from tradestation_client import TradeStationSimpleClient
from greeks_calculator import GreeksCalculator
from src.utils import get_logger

logger = get_logger(__name__)


class StreamingIngestionEngine:
    """Ingest real-time options data from TradeStation streaming API"""

    def __init__(self, config_path: str = "config/ingestion_config.yaml"):
        """
        Initialize ingestion engine

        Args:
            config_path: Path to configuration YAML file
        """
        logger.info("🔄 Initializing Streaming Ingestion Engine...")

        # Load environment variables from .env file FIRST
        from dotenv import load_dotenv
        env_file = Path(__file__).parent.parent.parent / ".env"
        load_dotenv(env_file)
        logger.debug(f"Environment variables loaded from {env_file}")

        # Load configuration (will now have access to env vars)
        self.config = self._load_config(config_path)
        logger.info(f"✅ Configuration loaded from {config_path}")

        # Load database credentials from ~/.zerogex_db_creds
        db_creds = self._load_db_credentials()
        logger.info("✅ Database credentials loaded from ~/.zerogex_db_creds")

        # Database connection
        logger.debug("Connecting to database...")
        try:
            self.db_conn = psycopg2.connect(
                host=db_creds['host'],
                database=db_creds['name'],
                user=db_creds['user'],
                password=db_creds['password'],
                port=db_creds['port']
            )
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.critical(f"Failed to connect to database: {e}", exc_info=True)
            raise

        # TradeStation credentials from environment variables
        self.ts_client_id = os.getenv('TRADESTATION_CLIENT_ID')
        self.ts_client_secret = os.getenv('TRADESTATION_CLIENT_SECRET')
        self.ts_refresh_token = os.getenv('TRADESTATION_REFRESH_TOKEN')
        self.ts_sandbox = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'

        # Greeks calculator
        self.greeks_calc = GreeksCalculator(
            risk_free_rate=self.config['greeks']['risk_free_rate'],
            dividend_yield=self.config['greeks']['dividend_yield']
        )
        logger.info("✅ Greeks calculator initialized")

        # Statistics tracking
        self.stats = {
            'options_received': 0,
            'options_stored': 0,
            'errors': 0,
            'heartbeats': 0,
            'underlying_updates': 0,
            'last_heartbeat': None,
            'start_time': datetime.now(timezone.utc)
        }

        # Batch processing
        self.batch_size = self.config['ingestion']['batch_size']
        self.batch_buffer = []
        self.batch_lock = asyncio.Lock()

        # Underlying price cache
        self.underlying_prices = {}

        logger.info("✅ Streaming Ingestion Engine initialized")
        logger.info(f"   Symbols: {', '.join(self.config['symbols'])}")
        logger.info(f"   Batch size: {self.batch_size}")
        logger.info(f"   Greeks calculation: ENABLED")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        logger.debug(f"Loading configuration from {config_path}...")

        config_file = Path(config_path)
        if not config_file.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        logger.debug("Configuration loaded successfully")
        return config

    def _load_db_credentials(self) -> Dict:
        """Load database credentials from ~/.zerogex_db_creds"""
        creds_file = Path.home() / ".zerogex_db_creds"

        if not creds_file.exists():
            logger.error(f"Database credentials file not found: {creds_file}")
            raise FileNotFoundError(f"Database credentials file not found: {creds_file}")

        logger.debug(f"Loading database credentials from {creds_file}...")

        creds = {}
        with open(creds_file, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    creds[key] = value

        # Map the keys to what we need
        db_config = {
            'host': creds.get('DB_HOST', 'localhost'),
            'port': int(creds.get('DB_PORT', '5432')),
            'name': creds.get('DB_NAME', 'gex_db'),
            'user': creds.get('DB_USER', 'gex_user'),
            'password': creds.get('DB_PASSWORD', ''),
        }

        logger.debug("Database credentials parsed successfully")
        return db_config

    async def option_update_handler(self, data: Dict, symbol: str, expiration: date):
        """
        Handle incoming option update from stream

        Args:
            data: Option data from stream
            symbol: Underlying symbol
            expiration: Expiration date
        """
        self.stats['options_received'] += 1

        try:
            # Check for heartbeat
            if 'Heartbeat' in data:
                self.stats['heartbeats'] = data['Heartbeat']
                self.stats['last_heartbeat'] = datetime.fromisoformat(
                    data['Timestamp'].replace('Z', '+00:00')
                )
                logger.debug(f"💓 Heartbeat #{self.stats['heartbeats']}")
                return

            # Parse option data
            option = self._parse_option_update(data, symbol, expiration)

            if not option:
                logger.debug("Failed to parse option data")
                return

            # Add to batch buffer
            async with self.batch_lock:
                self.batch_buffer.append(option)

                # Flush if batch is full
                if len(self.batch_buffer) >= self.batch_size:
                    logger.info(f"Batch full ({len(self.batch_buffer)} records), flushing...")
                    await self._flush_batch()

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error handling option update: {e}", exc_info=True)

    def _parse_option_update(self, data: Dict, symbol: str, expiration: date) -> Dict:
        """
        Parse raw option data from stream

        Args:
            data: Raw option data
            symbol: Underlying symbol
            expiration: Expiration date

        Returns:
            Parsed option dictionary or None
        """
        try:
            # Extract leg info
            if not data.get('Legs') or len(data['Legs']) == 0:
                return None

            leg = data['Legs'][0]

            # Parse fields
            option_symbol = leg.get('Symbol')
            if not option_symbol:
                return None

            strike = float(leg.get('StrikePrice', 0))
            option_type = leg.get('OptionType', '').lower()

            # Get underlying price
            underlying_price = self.underlying_prices.get(symbol, 0)

            # Calculate DTE
            dte = (expiration - date.today()).days

            # Build option dict
            option = {
                'timestamp': datetime.now(timezone.utc),
                'symbol': option_symbol,
                'underlying': symbol,
                'underlying_price': underlying_price,
                'strike': strike,
                'expiration': expiration,
                'dte': dte,
                'option_type': option_type,
                'bid': float(data.get('Bid', 0)),
                'ask': float(data.get('Ask', 0)),
                'mid': float(data.get('Mid', 0)),
                'last': float(data.get('Last', 0)),
                'volume': int(data.get('Volume', 0)),
                'open_interest': int(data.get('DailyOpenInterest', 0)),
                'implied_vol': float(data.get('ImpliedVolatility', 0)),
                'source': 'tradestation_stream'
            }

            # Calculate spread percentage
            if option['bid'] > 0 and option['ask'] > 0:
                option['spread_pct'] = (
                    (option['ask'] - option['bid']) / option['mid'] 
                    if option['mid'] > 0 else 0
                )
            else:
                option['spread_pct'] = None

            # Calculate Greeks
            if underlying_price > 0 and option['implied_vol'] > 0:
                greeks = self.greeks_calc.calculate_greeks(
                    underlying_price=underlying_price,
                    strike=strike,
                    expiration=expiration,
                    option_type=option_type,
                    implied_vol=option['implied_vol']
                )
                option.update(greeks)
                option['is_calculated'] = True
            else:
                # Use TradeStation Greeks if available
                option['delta'] = float(data.get('Delta', 0))
                option['gamma'] = float(data.get('Gamma', 0))
                option['theta'] = float(data.get('Theta', 0))
                option['vega'] = float(data.get('Vega', 0))
                option['rho'] = float(data.get('Rho', 0))
                option['is_calculated'] = False

            return option

        except Exception as e:
            logger.error(f"Error parsing option data: {e}", exc_info=True)
            return None

    async def _flush_batch(self):
        """Flush batch buffer to database"""
        if not self.batch_buffer:
            return

        start_time = time.time()
        batch_size = len(self.batch_buffer)

        logger.debug(f"Flushing batch of {batch_size} options...")

        try:
            self._store_options_batch(self.batch_buffer)

            processing_time_ms = int((time.time() - start_time) * 1000)

            self.stats['options_stored'] += batch_size

            # Clear buffer
            self.batch_buffer.clear()

            logger.info(f"✅ Batch stored successfully ({self.stats['options_stored']} total)")
            logger.debug(f"   Processing time: {processing_time_ms}ms")

        except Exception as e:
            logger.error(f"Failed to flush batch: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise

    def _store_options_batch(self, batch: List[Dict]):
        """Store batch of options to database"""
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
                opt['source']
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
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            self.db_conn.rollback()
            raise
        finally:
            cursor.close()

    def _store_underlying_quote(self, symbol: str, quote: Dict):
        """Store underlying price quote"""
        cursor = self.db_conn.cursor()

        insert_query = """
            INSERT INTO underlying_quotes 
            (timestamp, symbol, open, close, high, low, 
             total_volume, up_volume, down_volume, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cursor.execute(insert_query, (
                datetime.now(timezone.utc),
                symbol,
                quote['open'],
                quote['close'],
                quote['high'],
                quote['low'],
                quote['total_vol'],
                quote['up_vol'],
                quote['down_vol'],
                'tradestation_api'
            ))
            self.db_conn.commit()

            # Update cache
            self.underlying_prices[symbol] = quote['close']

            logger.debug(f"Stored underlying quote: {symbol} = ${quote['close']:.2f}")

        except Exception as e:
            logger.error(f"Error storing underlying quote: {e}")
            self.db_conn.rollback()
        finally:
            cursor.close()

    def _log_ingestion_metrics(self):
        """Log ingestion metrics to database"""
        cursor = self.db_conn.cursor()

        insert_query = """
            INSERT INTO ingestion_metrics 
            (timestamp, source, symbol, records_ingested, records_stored, 
             error_count, heartbeat_count, last_heartbeat, processing_time_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            # Calculate uptime
            uptime_seconds = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()

            for symbol in self.config['symbols']:
                cursor.execute(insert_query, (
                    datetime.now(timezone.utc),
                    'tradestation_stream',
                    symbol,
                    self.stats['options_received'],
                    self.stats['options_stored'],
                    self.stats['errors'],
                    self.stats['heartbeats'],
                    self.stats['last_heartbeat'],
                    int(uptime_seconds * 1000)
                ))

            self.db_conn.commit()
            logger.debug(f"📊 Logged ingestion metrics")

        except Exception as e:
            logger.error(f"Failed to log ingestion metrics: {e}")
            self.db_conn.rollback()
        finally:
            cursor.close()

    async def update_underlying_quotes(self):
        """Periodically update underlying quotes"""
        update_interval = self.config['ingestion']['underlying_update_interval']

        logger.info(f"Starting underlying quotes updater (interval: {update_interval}s)")

        # Create REST client for quotes
        rest_client = TradeStationSimpleClient(
            self.ts_client_id,
            self.ts_client_secret,
            self.ts_refresh_token,
            self.ts_sandbox
        )

        while True:
            try:
                for symbol in self.config['symbols']:
                    quote = rest_client.get_quote(symbol=symbol)

                    if quote:
                        self._store_underlying_quote(symbol, quote)
                        self.stats['underlying_updates'] += 1
                    else:
                        logger.warning(f"Failed to get quote for {symbol}")

                await asyncio.sleep(update_interval)

            except asyncio.CancelledError:
                logger.info("Underlying quotes updater stopped")
                break
            except Exception as e:
                logger.error(f"Error updating underlying quotes: {e}", exc_info=True)
                await asyncio.sleep(update_interval)

    async def log_metrics_periodically(self):
        """Periodically log ingestion metrics"""
        metrics_interval = self.config['ingestion']['metrics_interval']

        logger.info(f"Starting metrics logger (interval: {metrics_interval}s)")

        while True:
            try:
                await asyncio.sleep(metrics_interval)
                self._log_ingestion_metrics()

            except asyncio.CancelledError:
                logger.info("Metrics logger stopped")
                break
            except Exception as e:
                logger.error(f"Error logging metrics: {e}", exc_info=True)

    async def run(self):
        """Main ingestion loop"""
        logger.info("="*60)
        logger.info("Starting Streaming Ingestion Engine")
        logger.info("="*60)

        symbols = self.config['symbols']
        target_expiration = self.config['ingestion']['target_expiration']

        logger.info(f"Symbols: {', '.join(symbols)}")
        logger.info(f"Target expiration: {target_expiration}")

        async with TradeStationStreamingClient(
            self.ts_client_id,
            self.ts_client_secret,
            self.ts_refresh_token,
            self.ts_sandbox
        ) as stream_client:

            # Get expiration date
            if target_expiration == 'today':
                expiration = date.today()
                logger.info(f"Using today's expiration: {expiration}")
            else:
                expiration = datetime.strptime(target_expiration, '%Y-%m-%d').date()
                logger.info(f"Using specified expiration: {expiration}")

            # Start background tasks
            tasks = []

            # Start underlying quotes updater
            underlying_task = asyncio.create_task(self.update_underlying_quotes())
            tasks.append(underlying_task)

            # Start metrics logger
            metrics_task = asyncio.create_task(self.log_metrics_periodically())
            tasks.append(metrics_task)

            # Start streaming for each symbol
            for symbol in symbols:
                logger.info(f"🚀 Starting stream for {symbol} {expiration}")

                # Create callback for this symbol using closure factory
                def make_callback(sym, exp):
                    async def callback(data):
                        await self.option_update_handler(data, sym, exp)
                    return callback

                callback_func = make_callback(symbol, expiration)

                stream_task = asyncio.create_task(
                    stream_client.stream_options_chain(
                        underlying=symbol,
                        expiration=expiration.strftime('%Y-%m-%d'),
                        callback=callback_func,
                        strike_proximity=self.config['ingestion'].get('strike_proximity')
                    )
                )
                tasks.append(stream_task)

            # Run until cancelled
            try:
                await asyncio.gather(*tasks)
            except KeyboardInterrupt:
                logger.info("Shutting down...")

                # Cancel all tasks
                for task in tasks:
                    task.cancel()

                # Wait for tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)

                # Flush remaining data
                logger.info("Flushing remaining batch data...")
                await self._flush_batch()

                # Log final metrics
                self._log_ingestion_metrics()

        self.db_conn.close()

        logger.info("="*60)
        logger.info("Ingestion Engine Stopped")
        logger.info(f"Final Stats:")
        logger.info(f"  Options received: {self.stats['options_received']}")
        logger.info(f"  Options stored: {self.stats['options_stored']}")
        logger.info(f"  Underlying updates: {self.stats['underlying_updates']}")
        logger.info(f"  Errors: {self.stats['errors']}")
        logger.info("="*60)


async def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Streaming Options Ingestion Engine')
    parser.add_argument(
        '--config', 
        default='config/ingestion_config.yaml',
        help='Path to configuration file'
    )
    args = parser.parse_args()

    engine = StreamingIngestionEngine(config_path=args.config)
    await engine.run()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
