"""
GEX Calculation Scheduler

Runs GEX calculations periodically during market hours.
Fetches fresh underlying prices for accurate calculations.
"""

import asyncio
import psycopg2
from datetime import datetime, time as dt_time
import pytz
import logging
import os
import sys
from dotenv import load_dotenv

# Add parent directories to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'gex'))

from gex_calculator import GEXCalculator
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


class GEXScheduler:
    """Schedule and run GEX calculations with fresh price data"""
    
    def __init__(self, interval_seconds: int = 60):
        """
        Args:
            interval_seconds: How often to calculate GEX (default 60s)
        """
        logger.info(f"Initializing GEX Scheduler (interval: {interval_seconds}s)")
        
        self.interval = interval_seconds
        
        # Database connection
        try:
            self.db_conn = self._connect_db()
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.critical(f"Failed to connect to database: {e}", exc_info=True)
            raise
        
        self.calculator = GEXCalculator(self.db_conn)
        
        # TradeStation credentials
        self.ts_client_id = os.getenv('TRADESTATION_CLIENT_ID')
        self.ts_client_secret = os.getenv('TRADESTATION_CLIENT_SECRET')
        self.ts_refresh_token = os.getenv('TRADESTATION_REFRESH_TOKEN')
        self.ts_sandbox = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
        
        if not all([self.ts_client_id, self.ts_client_secret, self.ts_refresh_token]):
            logger.critical("Missing TradeStation credentials")
            raise ValueError("Missing required TradeStation credentials")
        
        logger.debug("TradeStation credentials loaded")
        logger.info("GEX Scheduler initialized successfully")
    
    def _connect_db(self):
        """Connect to database"""
        logger.debug("Connecting to database...")
        
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
    
    def is_market_open(self) -> bool:
        """Check if market is open"""
        now = datetime.now(pytz.timezone('America/New_York'))
        
        logger.debug(f"Market check: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.debug(f"Day: {now.strftime('%A')} (weekday: {now.weekday()})")
        
        if now.weekday() >= 5:
            logger.debug("Market closed: Weekend")
            return False
        
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        current_time = now.time()
        
        is_open = market_open <= current_time <= market_close
        
        logger.debug(f"Market hours: {market_open}-{market_close}, Current: {current_time}, Open: {is_open}")
        
        return is_open
    
    async def run(self, symbol: str = 'SPY'):
        """Main scheduler loop"""
        
        logger.info("="*60)
        logger.info(f"Starting GEX Scheduler for {symbol}")
        logger.info("="*60)
        
        calculations = 0
        errors = 0
        
        async with TradeStationStreamingClient(
            self.ts_client_id,
            self.ts_client_secret,
            self.ts_refresh_token,
            sandbox=self.ts_sandbox
        ) as client:
            
            logger.info("TradeStation client initialized")
            
            while True:
                try:
                    if not self.is_market_open():
                        logger.info("Market closed, waiting 5 minutes...")
                        await asyncio.sleep(300)
                        continue
                    
                    logger.debug(f"Starting GEX calculation cycle #{calculations + 1}")
                    
                    # Get fresh quote from TradeStation
                    logger.debug(f"Fetching fresh quote for {symbol}...")
                    quote = await client.get_quote(symbol)
                    
                    if quote:
                        current_price = quote['price']
                        logger.info(f"Fresh {symbol} price: ${current_price:.2f}")
                    else:
                        logger.warning("Could not get quote, will use price from data")
                        current_price = None
                        errors += 1
                    
                    logger.info(f"Calculating GEX for {symbol}...")
                    
                    # Calculate GEX with fresh price
                    metrics = self.calculator.calculate_current_gex(symbol, current_price)
                    
                    if metrics:
                        calculations += 1
                        logger.info(f"✅ GEX Calculation #{calculations}")
                        logger.info(f"   Spot: ${metrics.underlying_price:.2f}")
                        logger.info(f"   Total GEX: ${metrics.total_gamma_exposure/1e6:.1f}M")
                        logger.info(f"   Net GEX: ${metrics.net_gex/1e6:.1f}M")
                        logger.info(f"   Max Gamma: ${metrics.max_gamma_strike:.2f}")
                        if metrics.gamma_flip_point:
                            logger.info(f"   Flip Point: ${metrics.gamma_flip_point:.2f}")
                        
                        logger.debug(f"   P/C Ratio: {metrics.put_call_ratio:.2f}")
                        logger.debug(f"   Contracts: {metrics.total_contracts:,}")
                        
                        if calculations % 10 == 0:
                            logger.info(f"📊 Stats - Calculations: {calculations}, Errors: {errors}")
                    else:
                        logger.warning("No GEX metrics calculated")
                        errors += 1
                        
                        if errors % 5 == 0:
                            logger.warning(f"Error count: {errors}")
                    
                    logger.debug(f"Sleeping for {self.interval} seconds...")
                    await asyncio.sleep(self.interval)
                    
                except KeyboardInterrupt:
                    logger.info("Shutting down...")
                    break
                except Exception as e:
                    errors += 1
                    logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                    
                    if errors > 20:
                        logger.critical(f"Too many errors ({errors}), may indicate serious issue")
                    
                    wait_time = min(self.interval * 2, 300)  # Max 5 minutes
                    logger.info(f"Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
        
        self.db_conn.close()
        logger.info("="*60)
        logger.info("GEX Scheduler stopped")
        logger.info(f"Final stats - Calculations: {calculations}, Errors: {errors}")
        logger.info("="*60)


async def main():
    interval = int(os.getenv('POLL_INTERVAL', 60))
    logger.info(f"Starting with interval: {interval}s")
    
    scheduler = GEXScheduler(interval_seconds=interval)
    await scheduler.run('SPY')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
