"""
GEX Calculation Scheduler

Runs GEX calculations periodically during market hours with fresh underlying prices.
"""

import asyncio
import psycopg2
from datetime import datetime, date, time as dt_time
import pytz
import os
import sys
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.gex.gex_calculator import GEXCalculator
from src.utils import get_logger

# Load environment
env_file = project_root / ".env"
load_dotenv(env_file)

logger = get_logger(__name__)


class GEXScheduler:
    """Schedule and run GEX calculations with fresh price data"""

    def __init__(
        self,
        interval_seconds: int = 60,
        symbols: list = None,
        target_expiration: str = 'today'
    ):
        """
        Initialize GEX scheduler

        Args:
            interval_seconds: Calculation interval (default 60s)
            symbols: List of symbols to calculate (default ['SPY'])
            target_expiration: 'today' for 0DTE or specific date 'YYYY-MM-DD'
        """
        logger.info(f"Initializing GEX Scheduler...")
        logger.info(f"  Interval: {interval_seconds}s")

        self.interval = interval_seconds
        self.symbols = symbols or ['SPY']
        self.target_expiration = target_expiration

        logger.info(f"  Symbols: {', '.join(self.symbols)}")
        logger.info(f"  Target expiration: {target_expiration}")

        # Load database credentials
        db_creds = self._load_db_credentials()

        # Database connection
        try:
            self.db_conn = psycopg2.connect(**db_creds)
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.critical(f"Failed to connect to database: {e}", exc_info=True)
            raise

        # GEX calculator
        self.calculator = GEXCalculator(self.db_conn)

        # Statistics
        self.stats = {
            'calculations': 0,
            'errors': 0,
            'start_time': datetime.now(pytz.timezone('America/New_York'))
        }

        logger.info("✅ GEX Scheduler initialized successfully")

    def _load_db_credentials(self) -> dict:
        """Load database credentials from ~/.zerogex_db_creds"""
        creds_file = Path.home() / ".zerogex_db_creds"

        if not creds_file.exists():
            logger.critical(f"Database credentials not found: {creds_file}")
            raise FileNotFoundError(f"Database credentials not found: {creds_file}")

        logger.debug(f"Loading database credentials from {creds_file}...")

        creds = {}
        with open(creds_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    creds[key] = value

        return {
            'host': creds.get('DB_HOST', 'localhost'),
            'port': int(creds.get('DB_PORT', '5432')),
            'database': creds.get('DB_NAME', 'gex_db'),
            'user': creds.get('DB_USER', 'gex_user'),
            'password': creds.get('DB_PASSWORD', ''),
        }

    def is_market_open(self) -> bool:
        """
        Check if US stock market is open

        Returns:
            True if market is open, False otherwise
        """
        et_tz = pytz.timezone('America/New_York')
        now_et = datetime.now(et_tz)

        # Weekend check
        if now_et.weekday() >= 5:
            logger.debug(f"Market closed: Weekend ({now_et.strftime('%A')})")
            return False

        # Market hours: 9:30 AM - 4:00 PM ET
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        current_time = now_et.time()

        is_open = market_open <= current_time <= market_close

        logger.debug(f"Market check: {now_et.strftime('%H:%M %Z')} - "
                    f"{'OPEN' if is_open else 'CLOSED'}")

        return is_open

    def get_expiration_date(self) -> date:
        """
        Get expiration date based on target_expiration setting

        Returns:
            Date object for expiration
        """
        if self.target_expiration == 'today':
            return date.today()
        else:
            try:
                return datetime.strptime(self.target_expiration, '%Y-%m-%d').date()
            except ValueError:
                logger.error(f"Invalid expiration date: {self.target_expiration}")
                return date.today()

    def get_latest_underlying_price(self, symbol: str) -> Optional[float]:
        """
        Get latest underlying price from database

        Args:
            symbol: Symbol to query (e.g., 'SPY')

        Returns:
            Latest price or None if not found
        """
        cursor = self.db_conn.cursor()

        try:
            cursor.execute("""
                SELECT close
                FROM underlying_quotes
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))

            row = cursor.fetchone()

            if row:
                price = float(row[0])
                logger.debug(f"Latest {symbol} price from DB: ${price:.2f}")
                return price
            else:
                logger.warning(f"No underlying price found in DB for {symbol}")
                return None

        except Exception as e:
            logger.error(f"Error fetching underlying price: {e}", exc_info=True)
            return None
        finally:
            cursor.close()

    async def calculate_gex_for_symbol(self, symbol: str) -> bool:
        """
        Calculate GEX for a single symbol using database data

        Args:
            symbol: Symbol to calculate (e.g., 'SPY')

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get latest price from database
            logger.debug(f"Fetching latest {symbol} price from database...")
            current_price = self.get_latest_underlying_price(symbol)

            if not current_price:
                logger.warning(f"No price data available for {symbol}")
                return False

            logger.info(f"Latest {symbol} price: ${current_price:.2f}")

            # Get expiration date
            expiration = self.get_expiration_date()

            # Calculate GEX
            logger.info(f"Calculating GEX for {symbol} exp {expiration}...")
            metrics = self.calculator.calculate_current_gex(
                symbol, 
                current_price, 
                expiration
            )

            if metrics:
                self.stats['calculations'] += 1
                logger.info(f"✅ GEX Calculation #{self.stats['calculations']}")
                logger.info(f"   {symbol}: ${metrics.underlying_price:.2f}")
                logger.info(f"   Total GEX: ${metrics.total_gex_millions:.1f}M")
                logger.info(f"   Net GEX: ${metrics.net_gex_millions:.1f}M")
                logger.info(f"   Regime: {metrics.gamma_regime}")

                return True
            else:
                logger.warning(f"No GEX metrics calculated for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Error calculating GEX for {symbol}: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False

    async def run_once(self):
        """Run one cycle of GEX calculations for all symbols"""
        logger.debug(f"Starting calculation cycle...")

        results = []
        for symbol in self.symbols:
            result = await self.calculate_gex_for_symbol(symbol)
            results.append((symbol, result))

        # Log summary
        successful = sum(1 for _, r in results if r)
        logger.info(f"Cycle complete: {successful}/{len(self.symbols)} successful")

        return all(r for _, r in results)

    async def run(self):
        """Main scheduler loop"""
        logger.info("="*60)
        logger.info("Starting GEX Scheduler")
        logger.info("="*60)
        logger.info(f"Symbols: {', '.join(self.symbols)}")
        logger.info(f"Interval: {self.interval}s")
        logger.info(f"Target expiration: {self.target_expiration}")
        logger.info("="*60)

        cycle_count = 0

        try:
            while True:
                cycle_count += 1

                # Check if market is open
                if not self.is_market_open():
                    logger.info("Market closed, waiting 5 minutes...")
                    await asyncio.sleep(300)
                    continue

                logger.info(f"Starting calculation cycle #{cycle_count}...")

                # Run calculations
                await self.run_once()

                # Log periodic stats
                if cycle_count % 10 == 0:
                    self._log_statistics()

                # Wait for next interval
                logger.debug(f"Sleeping for {self.interval} seconds...")
                await asyncio.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Fatal error in scheduler: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()

    def _log_statistics(self):
        """Log scheduler statistics"""
        uptime = datetime.now(pytz.timezone('America/New_York')) - self.stats['start_time']
        uptime_hours = uptime.total_seconds() / 3600

        logger.info("="*60)
        logger.info("SCHEDULER STATISTICS")
        logger.info("="*60)
        logger.info(f"Uptime: {uptime_hours:.1f} hours")
        logger.info(f"Total calculations: {self.stats['calculations']}")
        logger.info(f"Errors: {self.stats['errors']}")

        if uptime_hours > 0:
            calc_per_hour = self.stats['calculations'] / uptime_hours
            logger.info(f"Calculations/hour: {calc_per_hour:.1f}")

        logger.info("="*60)

    def _cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up...")

        try:
            if self.db_conn:
                self.db_conn.close()
                logger.info("✅ Database connection closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

        # Log final statistics
        logger.info("="*60)
        logger.info("FINAL STATISTICS")
        logger.info("="*60)
        logger.info(f"Total calculations: {self.stats['calculations']}")
        logger.info(f"Total errors: {self.stats['errors']}")
        logger.info("="*60)
        logger.info("GEX Scheduler stopped")


async def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='GEX Calculation Scheduler')
    parser.add_argument(
        '--interval',
        type=int,
        default=int(os.getenv('POLL_INTERVAL', 60)),
        help='Calculation interval in seconds (default: 60)'
    )
    parser.add_argument(
        '--symbols',
        nargs='+',
        default=['SPY'],
        help='Symbols to calculate GEX for (default: SPY)'
    )
    parser.add_argument(
        '--expiration',
        default='today',
        help='Target expiration: "today" or YYYY-MM-DD (default: today)'
    )

    args = parser.parse_args()

    logger.info(f"Starting with interval: {args.interval}s")
    logger.info(f"Symbols: {', '.join(args.symbols)}")
    logger.info(f"Expiration: {args.expiration}")

    scheduler = GEXScheduler(
        interval_seconds=args.interval,
        symbols=args.symbols,
        target_expiration=args.expiration
    )

    await scheduler.run()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
