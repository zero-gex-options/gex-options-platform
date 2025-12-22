"""
TradeStation Streaming Ingestion Engine

Streams 0DTE options data and stores to database.
Optionally validates TradeStation Greeks against calculated values.
"""

import asyncio
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, time as dt_time, date
import pytz
import logging
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

from tradestation_client import TradeStationStreamingClient
from greeks_calculator import GreeksCalculator

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamingIngestionEngine:
    """Ingestion engine for TradeStation streaming options data with Greeks validation"""
    
    def __init__(self):
        logger.info("Initializing Streaming Ingestion Engine")
        
        self.db_conn = self._connect_db()
        
        # TradeStation credentials
        self.ts_client_id = os.getenv('TRADESTATION_CLIENT_ID')
        self.ts_client_secret = os.getenv('TRADESTATION_CLIENT_SECRET')
        self.ts_refresh_token = os.getenv('TRADESTATION_REFRESH_TOKEN')
        self.ts_sandbox = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
        
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
        self.delta_tolerance = 0.10  # 10% (was 5%)
        self.gamma_tolerance = 0.25  # 25% (was 10%)
        self.vega_tolerance = 0.20   # 20% (was 10%)
        self.theta_tolerance = 0.25  # 25% (new)
        
        # Stats
        self.options_received = 0
        self.options_stored = 0
        self.errors = 0
        
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
        
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Sandbox mode: {self.ts_sandbox}")
        logger.info(f"Greeks validation: {'ENABLED' if self.validate_greeks else 'DISABLED'}")
    
    def _connect_db(self):
        """Connect to PostgreSQL/TimescaleDB"""
        logger.info("Connecting to database...")
        
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        logger.info("✅ Database connected")
        return conn

    def is_market_open(self) -> bool:
        """Check if market is currently open"""
        # Always use ET timezone
        et_tz = pytz.timezone('America/New_York')
        now = datetime.now(et_tz)

        # Log current time for debugging
        logger.debug(f"Current ET time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.debug(f"Day of week: {now.weekday()} (0=Mon, 6=Sun)")

        # Weekend check
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            logger.debug("Market closed: Weekend")
            return False

        # Market hours: 9:30 AM - 4:00 PM ET
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        current_time = now.time()

        is_open = market_open <= current_time <= market_close

        logger.debug(f"Market open: {market_open}, Close: {market_close}, Current: {current_time}")
        logger.debug(f"Is market open: {is_open}")

        return is_open

    async def option_update_handler(self, data: Dict):
        """Handle incoming option update from stream"""
        
        self.options_received += 1
        
        try:
            # Parse the option data
            option = self._parse_option_update(data)
            
            if not option:
                return
            
            # Validate Greeks if enabled
            if self.validate_greeks and option['implied_vol'] > 0:
                self._validate_greeks(option)
            
            # Add to batch buffer
            async with self.batch_lock:
                self.batch_buffer.append(option)
                
                # Flush batch if full
                if len(self.batch_buffer) >= self.batch_size:
                    logger.info(f"Batch full ({len(self.batch_buffer)} records), flushing...")
                    await self._flush_batch()
                    
        except Exception as e:
            logger.error(f"Error in option handler: {e}", exc_info=True)
            self.errors += 1
    
    def _parse_option_update(self, data: Dict) -> Optional[Dict]:
        """Parse TradeStation option update into standardized format"""
        
        try:
            # Extract leg info
            if not data.get('Legs') or len(data['Legs']) == 0:
                return None
            
            leg = data['Legs'][0]
            
            # Parse symbol
            symbol = leg.get('Symbol')
            if not symbol:
                return None
            
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
                'open_interest': int(data.get('DailyOpenInterest', 0)),
                'implied_vol': float(data.get('ImpliedVolatility', 0)),
                'delta': float(data.get('Delta', 0)),
                'gamma': float(data.get('Gamma', 0)),
                'theta': float(data.get('Theta', 0)),
                'vega': float(data.get('Vega', 0)),
                'rho': float(data.get('Rho', 0)),
                'timestamp': datetime.now(),
                'underlying_price': self.underlying_price,
                'dte': (expiration - date.today()).days,
                'is_calculated': False
            }
            
            # Calculate spread percentage
            if option['bid'] > 0 and option['ask'] > 0:
                option['spread_pct'] = (option['ask'] - option['bid']) / option['mid'] if option['mid'] > 0 else 0
            else:
                option['spread_pct'] = None
            
            return option
            
        except Exception as e:
            logger.error(f"Failed to parse option: {e}")
            return None

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
            return
        
        logger.info(f"Flushing {len(self.batch_buffer)} options to database")
        
        try:
            self._store_options_batch(self.batch_buffer)
            
            self.options_stored += len(self.batch_buffer)
            logger.info(f"✅ Stored {len(self.batch_buffer)} options (total: {self.options_stored})")
            
            self.batch_buffer.clear()
            
        except Exception as e:
            logger.error(f"Error flushing batch: {e}", exc_info=True)
            self.errors += 1
            self.batch_buffer.clear()
    
    def _store_options_batch(self, batch: List[Dict]):
        """Store batch of options to database"""
        
        cursor = self.db_conn.cursor()
        
        values = []
        for opt in batch:
            values.append((
                opt['timestamp'],
                opt['underlying'],
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
        
        execute_values(cursor, insert_query, values)
        self.db_conn.commit()
        cursor.close()
    
    def _store_underlying_price(self, symbol: str, price: float):
        """Store underlying price update"""
        
        cursor = self.db_conn.cursor()
        
        insert_query = """
            INSERT INTO underlying_prices 
            (timestamp, symbol, price, bid, ask, volume, source)
            VALUES (%s, %s, %s, %s, %s, %s, 'tradestation_stream')
            ON CONFLICT (timestamp, symbol) DO NOTHING
        """
        
        cursor.execute(insert_query, (
            datetime.now(),
            symbol,
            price,
            price,
            price,
            0
        ))
        self.db_conn.commit()
        cursor.close()
    
    async def run(self, symbol: str = 'SPY'):
        """Main ingestion loop"""
        
        logger.info("="*80)
        logger.info(f"Starting Streaming Ingestion Engine for {symbol}")
        if self.validate_greeks:
            logger.info("Greeks validation: ENABLED")
        logger.info("="*80)
        
        async with TradeStationStreamingClient(
            self.ts_client_id,
            self.ts_client_secret,
            self.ts_refresh_token,
            sandbox=self.ts_sandbox
        ) as client:
            
            while True:
                try:
                    if not self.is_market_open():
                        logger.info("Market closed, waiting 5 minutes...")
                        await asyncio.sleep(300)
                        continue
                    
                    logger.info("Market is open, starting stream...")
                    
                    # Get underlying price
                    quote = await client.get_quote(symbol)
                    if quote:
                        self.underlying_price = quote['price']
                        self._store_underlying_price(symbol, self.underlying_price)
                        logger.info(f"Underlying {symbol}: ${self.underlying_price:.2f}")

                    # Start streaming
                    await client.start_streaming_0dte(symbol, self.option_update_handler)

                    logger.info("🚀 Stream task started, waiting for connection...")

                    # Wait for stream to actually connect (up to 30 seconds)
                    connection_timeout = 30
                    elapsed = 0
                    while not client.is_streaming and elapsed < connection_timeout:
                        await asyncio.sleep(1)
                        elapsed += 1

                    if not client.is_streaming:
                        logger.error("Stream failed to connect within 30 seconds")
                        continue

                    logger.info("✅ Stream connected and active")

                    # Keep streaming while market is open
                    while self.is_market_open():
                        # Check if stream is still active
                        if not client.is_streaming:
                            logger.warning("Stream disconnected, will restart on next iteration")
                            break

                        # Log stats every minute
                        await asyncio.sleep(60)

                        stats = client.get_stats()
                        logger.info(
                            f"📊 Stats - Received: {self.options_received}, "
                            f"Stored: {self.options_stored}, "
                            f"Errors: {self.errors}, "
                            f"Messages: {stats['messages_received']}"
                        )

                        # Periodic flush
                        if self.batch_buffer:
                            async with self.batch_lock:
                                await self._flush_batch()

                    logger.info("Market closed or stream ended")
                    await client.stop_streaming()
                    
                    # Final flush
                    if self.batch_buffer:
                        async with self.batch_lock:
                            await self._flush_batch()
                    
                    if self.validate_greeks and self.greeks_validated > 0:
                        self._log_validation_summary()
                    
                except KeyboardInterrupt:
                    logger.info("Shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    await asyncio.sleep(30)
        
        self.db_conn.close()
        logger.info("Ingestion engine stopped")


async def main():
    engine = StreamingIngestionEngine()
    await engine.run('SPY')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
