"""
GEX Calculation Scheduler

Runs GEX calculations periodically during market hours.
"""

import asyncio
import psycopg2
from datetime import datetime, time as dt_time
import pytz
import logging
import os
from dotenv import load_dotenv

from gex_calculator import GEXCalculator

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GEXScheduler:
    """Schedule and run GEX calculations"""
    
    def __init__(self, interval_seconds: int = 60):
        """
        Args:
            interval_seconds: How often to calculate GEX (default 60s)
        """
        self.interval = interval_seconds
        self.db_conn = self._connect_db()
        self.calculator = GEXCalculator(self.db_conn)
        
        logger.info(f"GEX Scheduler initialized (interval: {interval_seconds}s)")
    
    def _connect_db(self):
        """Connect to database"""
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
        
        if now.weekday() >= 5:
            return False
        
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        current_time = now.time()
        
        return market_open <= current_time <= market_close
    
    async def run(self, symbol: str = 'SPY'):
        """Main scheduler loop"""
        
        logger.info("="*60)
        logger.info(f"Starting GEX Scheduler for {symbol}")
        logger.info("="*60)
        
        calculations = 0
        
        while True:
            try:
                if not self.is_market_open():
                    logger.info("Market closed, waiting 5 minutes...")
                    await asyncio.sleep(300)
                    continue
                
                logger.info(f"Calculating GEX for {symbol}...")
                
                metrics = self.calculator.calculate_current_gex(symbol)
                
                if metrics:
                    calculations += 1
                    logger.info(f"✅ GEX Calculation #{calculations}")
                    logger.info(f"   Spot: ${metrics.underlying_price:.2f}")
                    logger.info(f"   Total GEX: ${metrics.total_gamma_exposure/1e6:.1f}M")
                    logger.info(f"   Net GEX: ${metrics.net_gex/1e6:.1f}M")
                    logger.info(f"   Max Gamma: ${metrics.max_gamma_strike:.2f}")
                    if metrics.gamma_flip_point:
                        logger.info(f"   Flip Point: ${metrics.gamma_flip_point:.2f}")
                else:
                    logger.warning("No GEX metrics calculated")
                
                await asyncio.sleep(self.interval)
                
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error: {e}", exc_info=True)
                await asyncio.sleep(self.interval * 2)
        
        self.db_conn.close()
        logger.info("GEX Scheduler stopped")


async def main():
    interval = int(os.getenv('POLL_INTERVAL', 60))
    scheduler = GEXScheduler(interval_seconds=interval)
    await scheduler.run('SPY')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
