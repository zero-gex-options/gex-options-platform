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
        logger.info("Initializing Streaming Ingestion Engine")
        logger.debug(f"Batch size: {os.getenv('BATCH_SIZE', 100)}")
        
        # Database connection
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
        
        # Statistics
        self.options_received = 0
        self.options_stored = 0
        self.batch_count = 0
        self.error_count = 0
        
        logger.info("Streaming Ingestion Engine initialized")
    
    def is_market_open(self) -> bool:
        """Check if market is currently open"""
        now = datetime.now(pytz.timezone('America/New_York'))
        
        logger.debug(f"Market hours check: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.debug(f"Day: {now.strftime('%A')} (weekday: {now.weekday()})")
        
        if now.weekday() >= 5:
            logger.debug("Market closed: Weekend")
            return False
        
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
            logger.info(f"Options received: {self.options_received}, Stored: {self.options_stored}")
        else:
            logger.debug(f"Option update #{self.options_received}")
        
        try:
            # Parse the option data
            option = self._parse_option_update(data)
            
            if not option:
                logger.warning("Failed to parse option data")
                logger.debug(f"Invalid data: {data}")
                return
            
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
        
        logger.debug("Parsing option update")
        
        try:
            # Extract key fields
            symbol = data.get('Symbol', '')
            underlying = data.get('Underlying', '')
            strike = float(data.get('Strike', 0))
            expiration_str = data.get('ExpirationDate', '')
            
            logger.debug(f"Parsing: {symbol} (underlying: {underlying}, strike: {strike})")
            
            if not symbol or not underlying or strike == 0:
                logger.warning(f"Missing required fields in option data")
                return None
            
            # Determine option type
            if 'Call' in symbol or symbol.endswith('C'):
                option_type = 'call'
            elif 'Put' in symbol or symbol.endswith('P'):
                option_type = 'put'
            else:
                logger.warning(f"Could not determine option type for {symbol}")
                return None
            
            # Parse expiration
            try:
                expiration = datetime.strptime(expiration_str, '%Y-%m-%d').date()
            except ValueError:
                logger.error(f"Invalid expiration date format: {expiration_str}")
                return None
            
            # Calculate DTE
            dte = (expiration - date.today()).days
            
            # Get pricing
            bid = float(data.get('Bid', 0))
            ask = float(data.get('Ask', 0))
            last = float(data.get('Last', 0))
            mid = (bid + ask) / 2 if bid and ask else last
            
            # Greeks
            delta = float(data.get('Delta', 0))
            gamma = float(data.get('Gamma', 0))
            theta = float(data.get('Theta', 0))
            vega = float(data.get('Vega', 0))
            rho = float(data.get('Rho', 0))
            implied_vol = float(data.get('ImpliedVolatility', 0))
            
            # Volume and OI
            volume = int(data.get('Volume', 0))
            open_interest = int(data.get('OpenInterest', 0))
            
            # Underlying price
            underlying_price = float(data.get('UnderlyingPrice', 0))
            
            # Calculate spread percentage
            spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 0
            
            option_dict = {
                'timestamp': datetime.now(timezone.utc),
                'symbol': underlying,  # Store underlying, not full option symbol
                'underlying_price': underlying_price,
                'strike': strike,
                'expiration': expiration,
                'dte': dte,
                'option_type': option_type,
                'bid': bid,
                'ask': ask,
                'mid': mid,
                'last': last,
                'volume': volume,
                'open_interest': open_interest,
                'implied_vol': implied_vol,
                'delta': delta,
                'gamma': gamma,
                'theta': theta,
                'vega': vega,
                'rho': rho,
                'is_calculated': False,
                'spread_pct': spread_pct
            }
            
            logger.debug(f"✅ Parsed: {underlying} {strike}{option_type[0].upper()} (DTE={dte}, IV={implied_vol:.2f})")
            
            return option_dict
            
        except KeyError as e:
            logger.error(f"Missing key in option data: {e}")
            logger.debug(f"Data keys: {data.keys()}")
            return None
        except Exception as e:
            logger.error(f"Error parsing option data: {e}", exc_info=True)
            return None
    
    async def _flush_batch(self):
        """Flush batch buffer to database"""
        
        if not self.batch_buffer:
            logger.debug("Batch buffer empty, nothing to flush")
            return
        
        batch_size = len(self.batch_buffer)
        logger.info(f"Flushing batch of {batch_size} options to database")
        
        try:
            self._store_options_batch(self.batch_buffer)
            
            self.options_stored += batch_size
            self.batch_count += 1
            self.batch_buffer.clear()
            
            logger.info(f"✅ Batch #{self.batch_count} stored successfully ({self.options_stored} total options)")
            
            if self.batch_count % 10 == 0:
                logger.info(f"📊 Stats - Received: {self.options_received}, Stored: {self.options_stored}, "
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
            logger.error(f"Database error: {e}", exc_info=True)
            self.db_conn.rollback()
            raise
        finally:
            cursor.close()
    
    def _store_underlying_price(self, symbol: str, price: float):
        """Store underlying price"""
        
        logger.debug(f"Storing underlying price: {symbol} = ${price:.2f}")
        
        cursor = self.db_conn.cursor()
        
        insert_query = """
            INSERT INTO underlying_prices (timestamp, symbol, price, source)
            VALUES (%s, %s, %s, %s)
        """
        
        try:
            cursor.execute(insert_query, (
                datetime.now(timezone.utc),
                symbol,
                price,
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
        logger.info(f"Starting Streaming Ingestion for {symbol}")
        logger.info("="*60)
        
        while True:
            try:
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
                    
                    # Get current quote
                    logger.info(f"Getting quote for {symbol}...")
                    quote = await client.get_quote(symbol)
                    
                    if not quote:
                        logger.error(f"Failed to get quote for {symbol}")
                        await asyncio.sleep(60)
                        continue
                    
                    logger.info(f"✅ {symbol}: ${quote['price']:.2f}")
                    
                    # Store underlying price
                    self._store_underlying_price(symbol, quote['price'])
                    
                    # Get expirations
                    logger.info(f"Getting expirations for {symbol}...")
                    expirations = await client.get_option_expirations(symbol)
                    
                    if not expirations:
                        logger.error(f"No expirations found for {symbol}")
                        await asyncio.sleep(60)
                        continue
                    
                    # Find 0DTE
                    today = date.today().strftime('%Y-%m-%d')
                    
                    if today in expirations:
                        logger.info(f"✅ Found 0DTE expiration: {today}")
                        target_expiration = today
                    else:
                        logger.warning(f"No 0DTE expiration today, using nearest: {expirations[0]}")
                        target_expiration = expirations[0]
                    
                    # Start streaming
                    logger.info(f"🚀 Starting options stream for {symbol} {target_expiration}")
                    
                    await client.stream_options(
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
