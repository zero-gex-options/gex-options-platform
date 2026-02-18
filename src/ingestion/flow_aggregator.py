"""
Option Flow Aggregator

Aggregates streaming option data into 5-minute buckets to track:
- Premium spent
- Delta-weighted flow
- Notional value
- Buy/sell pressure
- Strike distribution
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from collections import defaultdict
from dataclasses import dataclass, field
import psycopg2
from psycopg2.extras import execute_values
from src.utils import get_logger

logger = get_logger(__name__)


@dataclass
class FlowBucket:
    """Accumulator for 5-minute flow data"""
    symbol: str
    option_type: str
    bucket_start: datetime
    bucket_end: datetime

    # Volume tracking
    total_volume: int = 0
    sweep_volume: int = 0
    block_volume: int = 0
    trade_count: int = 0

    # Premium tracking
    premium_sum: float = 0.0
    premium_volume_sum: float = 0.0  # For VWAP calculation

    # Notional tracking
    notional_sum: float = 0.0
    underlying_price_sum: float = 0.0
    price_count: int = 0

    # Delta-weighted tracking
    delta_weighted_sum: float = 0.0
    gamma_weighted_sum: float = 0.0

    # Flow direction
    buy_volume: int = 0
    sell_volume: int = 0

    # Strike distribution
    atm_volume: int = 0
    otm_volume: int = 0
    itm_volume: int = 0

    # Size metrics
    max_trade_size: int = 0
    unique_strikes: set = field(default_factory=set)

    # OI tracking
    oi_samples: List[int] = field(default_factory=list)

    def add_quote(self, quote: Dict):
        """Add an option quote to this bucket"""
        try:
            volume = quote.get('volume', 0)
            if volume <= 0:
                return

            # Update trade count
            self.trade_count += 1

            # Volume metrics
            self.total_volume += volume

            # Block trades (>= 100 contracts)
            if volume >= 100:
                self.block_volume += volume

            # Premium calculation
            mid = quote.get('mid', 0)
            if mid > 0:
                premium = mid * volume * 100  # Premium in dollars
                self.premium_sum += premium
                self.premium_volume_sum += premium * volume

            # Notional value
            underlying_price = quote.get('underlying_price', 0)
            if underlying_price > 0:
                notional = volume * underlying_price * 100
                self.notional_sum += notional
                self.underlying_price_sum += underlying_price
                self.price_count += 1

            # Delta-weighted flow
            delta = quote.get('delta', 0)
            if delta != 0:
                # For puts, delta is negative, so this naturally gives us
                # signed flow (positive for calls, negative for puts)
                self.delta_weighted_sum += volume * abs(delta) * underlying_price * 100

            # Gamma-weighted flow
            gamma = quote.get('gamma', 0)
            if gamma > 0:
                self.gamma_weighted_sum += volume * gamma

            # Estimate buy/sell based on bid/ask positioning
            bid = quote.get('bid', 0)
            ask = quote.get('ask', 0)
            last = quote.get('last', 0)

            if bid > 0 and ask > 0 and last > 0:
                # If last price is closer to ask, it's likely a buy
                # If closer to bid, it's likely a sell
                spread = ask - bid
                if spread > 0:
                    distance_from_bid = last - bid
                    pct_through_spread = distance_from_bid / spread

                    if pct_through_spread > 0.6:  # Closer to ask = buy
                        self.buy_volume += volume
                        # Check if it's a sweep (hitting the ask aggressively)
                        if pct_through_spread > 0.9:
                            self.sweep_volume += volume
                    elif pct_through_spread < 0.4:  # Closer to bid = sell
                        self.sell_volume += volume
                        # Check if it's a sweep (hitting the bid aggressively)
                        if pct_through_spread < 0.1:
                            self.sweep_volume += volume
                    else:
                        # Mid-spread, split it
                        self.buy_volume += volume // 2
                        self.sell_volume += volume - (volume // 2)

            # Strike distribution (ATM = within 2% of spot)
            strike = quote.get('strike', 0)
            if strike > 0 and underlying_price > 0:
                self.unique_strikes.add(strike)

                pct_diff = abs(strike - underlying_price) / underlying_price

                is_call = self.option_type == 'call'
                is_itm = (strike < underlying_price) if is_call else (strike > underlying_price)

                if pct_diff <= 0.02:  # Within 2%
                    self.atm_volume += volume
                elif is_itm:
                    self.itm_volume += volume
                else:
                    self.otm_volume += volume

            # Size tracking
            if volume > self.max_trade_size:
                self.max_trade_size = volume

            # OI sampling
            oi = quote.get('open_interest', 0)
            if oi > 0:
                self.oi_samples.append(oi)

        except Exception as e:
            logger.error(f"Error adding quote to flow bucket: {e}", exc_info=True)

    def to_db_row(self) -> tuple:
        """Convert bucket to database row"""
        # Calculate averages and derived metrics
        avg_premium = self.premium_sum / self.total_volume if self.total_volume > 0 else 0
        vwap_premium = self.premium_volume_sum / (self.total_volume ** 2) if self.total_volume > 0 else 0
        avg_underlying = self.underlying_price_sum / self.price_count if self.price_count > 0 else 0
        avg_trade_size = self.total_volume / self.trade_count if self.trade_count > 0 else 0
        net_flow = self.buy_volume - self.sell_volume

        # OI calculations
        starting_oi = self.oi_samples[0] if self.oi_samples else 0
        ending_oi = self.oi_samples[-1] if self.oi_samples else 0
        oi_change = ending_oi - starting_oi

        # Net delta exposure (sign depends on option type)
        net_delta = self.delta_weighted_sum
        if self.option_type == 'put':
            net_delta = -net_delta  # Puts have negative delta exposure

        return (
            # timestamp (rounded to 5-min bucket)
            self.bucket_start,
            # identifiers
            self.symbol,
            self.option_type,
            # volume
            self.total_volume,
            self.sweep_volume,
            self.block_volume,
            # OI
            oi_change,
            starting_oi,
            ending_oi,
            # premium
            self.premium_sum,
            avg_premium,
            vwap_premium,
            # notional
            self.notional_sum,
            avg_underlying,
            # delta-weighted
            self.delta_weighted_sum,
            net_delta,
            self.gamma_weighted_sum,
            # flow direction
            self.buy_volume,
            self.sell_volume,
            net_flow,
            # strike distribution
            self.atm_volume,
            self.otm_volume,
            self.itm_volume,
            # size metrics
            avg_trade_size,
            self.max_trade_size,
            self.trade_count,
            # metadata
            len(self.unique_strikes),
            self.bucket_start,
            self.bucket_end
        )


class OptionFlowAggregator:
    """Aggregate option quotes into 5-minute flow buckets"""

    BUCKET_INTERVAL_MINUTES = 5

    def __init__(self, db_conn):
        """
        Initialize flow aggregator

        Args:
            db_conn: PostgreSQL database connection
        """
        self.db_conn = db_conn

        # Active buckets: {(symbol, option_type, bucket_timestamp): FlowBucket}
        self.buckets: Dict[tuple, FlowBucket] = {}

        # Lock for thread-safe bucket access
        self.lock = asyncio.Lock()

        # Stats
        self.stats = {
            'quotes_processed': 0,
            'buckets_flushed': 0,
            'last_flush': datetime.now(timezone.utc)
        }

        logger.info(f"✅ OptionFlowAggregator initialized ({self.BUCKET_INTERVAL_MINUTES}-minute buckets)")

    def _get_bucket_timestamp(self, dt: datetime) -> datetime:
        """
        Round datetime down to nearest 5-minute bucket

        Args:
            dt: Datetime to round

        Returns:
            Rounded datetime
        """
        # Round down to nearest 5 minutes
        minutes = (dt.minute // self.BUCKET_INTERVAL_MINUTES) * self.BUCKET_INTERVAL_MINUTES
        return dt.replace(minute=minutes, second=0, microsecond=0)

    async def add_quote(self, quote: Dict):
        """
        Add an option quote to the appropriate bucket

        Args:
            quote: Option quote dictionary
        """
        try:
            # Extract key fields
            symbol = quote.get('underlying', 'UNKNOWN')
            option_type = quote.get('option_type', 'unknown').lower()
            timestamp = quote.get('timestamp', datetime.now(timezone.utc))

            # Get bucket timestamp
            bucket_ts = self._get_bucket_timestamp(timestamp)
            bucket_key = (symbol, option_type, bucket_ts)

            async with self.lock:
                # Create bucket if doesn't exist
                if bucket_key not in self.buckets:
                    bucket_end = bucket_ts + timedelta(minutes=self.BUCKET_INTERVAL_MINUTES)
                    self.buckets[bucket_key] = FlowBucket(
                        symbol=symbol,
                        option_type=option_type,
                        bucket_start=bucket_ts,
                        bucket_end=bucket_end
                    )

                # Add quote to bucket
                self.buckets[bucket_key].add_quote(quote)
                self.stats['quotes_processed'] += 1

        except Exception as e:
            logger.error(f"Error adding quote to flow aggregator: {e}", exc_info=True)

    async def flush_old_buckets(self, force_all: bool = False):
        """
        Flush completed buckets to database

        Args:
            force_all: If True, flush all buckets regardless of age
        """
        now = datetime.now(timezone.utc)
        current_bucket_ts = self._get_bucket_timestamp(now)

        buckets_to_flush = []

        async with self.lock:
            for key, bucket in list(self.buckets.items()):
                # Flush if bucket is complete (older than current bucket) or force_all
                if force_all or bucket.bucket_start < current_bucket_ts:
                    buckets_to_flush.append(bucket)
                    del self.buckets[key]

        if buckets_to_flush:
            self._flush_buckets_to_db(buckets_to_flush)
            self.stats['buckets_flushed'] += len(buckets_to_flush)
            self.stats['last_flush'] = now

            logger.info(
                f"💾 Flushed {len(buckets_to_flush)} flow buckets "
                f"(total: {self.stats['buckets_flushed']})"
            )

    def _flush_buckets_to_db(self, buckets: List[FlowBucket]):
        """
        Flush flow buckets to database

        Args:
            buckets: List of FlowBucket objects to flush
        """
        if not buckets:
            return

        cursor = self.db_conn.cursor()

        try:
            # Convert buckets to rows
            rows = [bucket.to_db_row() for bucket in buckets]

            insert_query = """
                INSERT INTO option_flow_metrics
                (timestamp, symbol, option_type,
                 total_volume, sweep_volume, block_volume,
                 oi_change, starting_oi, ending_oi,
                 total_premium, avg_premium, vwap_premium,
                 total_notional, avg_underlying_price,
                 delta_weighted_volume, net_delta_exposure, gamma_weighted_volume,
                 buy_volume, sell_volume, net_flow,
                 atm_volume, otm_volume, itm_volume,
                 avg_trade_size, max_trade_size, trade_count,
                 unique_strikes, bucket_start, bucket_end)
                VALUES %s
                ON CONFLICT (timestamp, symbol, option_type) DO UPDATE SET
                    total_volume = EXCLUDED.total_volume,
                    sweep_volume = EXCLUDED.sweep_volume,
                    block_volume = EXCLUDED.block_volume,
                    oi_change = EXCLUDED.oi_change,
                    starting_oi = EXCLUDED.starting_oi,
                    ending_oi = EXCLUDED.ending_oi,
                    total_premium = EXCLUDED.total_premium,
                    avg_premium = EXCLUDED.avg_premium,
                    vwap_premium = EXCLUDED.vwap_premium,
                    total_notional = EXCLUDED.total_notional,
                    avg_underlying_price = EXCLUDED.avg_underlying_price,
                    delta_weighted_volume = EXCLUDED.delta_weighted_volume,
                    net_delta_exposure = EXCLUDED.net_delta_exposure,
                    gamma_weighted_volume = EXCLUDED.gamma_weighted_volume,
                    buy_volume = EXCLUDED.buy_volume,
                    sell_volume = EXCLUDED.sell_volume,
                    net_flow = EXCLUDED.net_flow,
                    atm_volume = EXCLUDED.atm_volume,
                    otm_volume = EXCLUDED.otm_volume,
                    itm_volume = EXCLUDED.itm_volume,
                    avg_trade_size = EXCLUDED.avg_trade_size,
                    max_trade_size = EXCLUDED.max_trade_size,
                    trade_count = EXCLUDED.trade_count,
                    unique_strikes = EXCLUDED.unique_strikes,
                    bucket_start = EXCLUDED.bucket_start,
                    bucket_end = EXCLUDED.bucket_end
            """

            execute_values(cursor, insert_query, rows)
            self.db_conn.commit()

            logger.debug(f"Stored {len(rows)} flow buckets to database")

        except Exception as e:
            logger.error(f"Failed to flush flow buckets to database: {e}", exc_info=True)
            self.db_conn.rollback()
        finally:
            cursor.close()

    async def periodic_flush_task(self, interval_seconds: int = 60):
        """
        Background task to periodically flush completed buckets

        Args:
            interval_seconds: How often to check for completed buckets
        """
        logger.info(f"Starting periodic flush task (interval: {interval_seconds}s)")

        while True:
            try:
                await asyncio.sleep(interval_seconds)
                await self.flush_old_buckets()

                # Log stats every 10 flushes
                if self.stats['buckets_flushed'] % 10 == 0:
                    logger.info(
                        f"📊 Flow aggregator stats: "
                        f"{self.stats['quotes_processed']:,} quotes processed, "
                        f"{self.stats['buckets_flushed']} buckets flushed, "
                        f"{len(self.buckets)} active buckets"
                    )

            except asyncio.CancelledError:
                logger.info("Periodic flush task stopped, flushing remaining buckets...")
                await self.flush_old_buckets(force_all=True)
                break
            except Exception as e:
                logger.error(f"Error in periodic flush task: {e}", exc_info=True)

    def get_stats(self) -> Dict:
        """Get aggregator statistics"""
        return {
            **self.stats,
            'active_buckets': len(self.buckets),
            'bucket_interval_minutes': self.BUCKET_INTERVAL_MINUTES
        }
