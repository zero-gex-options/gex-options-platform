"""
Gamma Exposure (GEX) Calculator

Calculates dealer gamma exposure from options market data.
"""

import psycopg2
from datetime import datetime, date, timezone
import pytz
from typing import Dict, Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class GEXMetrics:
    """Container for GEX calculation results"""
    symbol: str
    expiration: date
    timestamp: datetime
    underlying_price: float
    total_gamma_exposure: float
    call_gamma: float
    put_gamma: float
    net_gex: float
    call_volume: int
    put_volume: int
    call_oi: int
    put_oi: int
    total_contracts: int
    max_gamma_strike: float
    max_gamma_value: float
    gamma_flip_point: Optional[float]
    put_call_ratio: float
    vanna_exposure: float
    charm_exposure: float


class GEXCalculator:
    """Calculate gamma exposure metrics from options data"""

    def __init__(self, db_connection):
        """
        Args:
            db_connection: psycopg2 database connection
        """
        self.db = db_connection

    def calculate_current_gex(self, symbol: str, current_price: Optional[float] = None) -> Optional[GEXMetrics]:
        """
        Calculate GEX for the most recent data

        Args:
            symbol: Underlying symbol (e.g., 'SPY')
            current_price: Optional current price (if not provided, uses price from data)

        Returns:
            GEXMetrics object or None if no data
        """
        cursor = self.db.cursor()

        # Get the most recent data for each strike for today's 0DTE
        query = """
            SELECT DISTINCT ON (strike, option_type)
                strike,
                option_type,
                gamma,
                delta,
                vega,
                open_interest,
                volume,
                underlying_price,
                expiration,
                timestamp
            FROM options_quotes
            WHERE symbol = %s
                AND DATE(expiration) = CURRENT_DATE
            ORDER BY strike, option_type, timestamp DESC
        """

        logger.info(f"Querying for {symbol} options with expiration {date.today()}")

        try:
            cursor.execute(query, (symbol,))
            rows = cursor.fetchall()

            logger.info(f"Query returned {len(rows)} option contracts")

            if not rows:
                logger.warning(f"No options data for {symbol} with today's expiration")

                # Debug
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT DATE(expiration)) as exp_dates,
                        MIN(DATE(expiration)) as min_exp,
                        MAX(DATE(expiration)) as max_exp,
                        MAX(timestamp) as latest_timestamp
                    FROM options_quotes
                    WHERE symbol = %s
                """, (symbol,))

                debug = cursor.fetchone()
                logger.info(f"Debug - Total: {debug[0]}, Exp dates: {debug[1]}, "
                           f"Range: {debug[2]} to {debug[3]}, Latest: {debug[4]}")

                cursor.close()
                return None

            # Parse data
            options_data = []
            expiration = rows[0][8]

            # Store timestamp in UTC for database
            timestamp = datetime.now(timezone.utc)

            # Use provided current price, or fallback to price from data
            if current_price is not None:
                underlying_price = current_price
                logger.info(f"Using provided current price: ${underlying_price:.2f}")
            else:
                underlying_price = rows[0][7]
                logger.info(f"Using price from options data: ${underlying_price:.2f}")

            logger.info(f"Expiration: {expiration}, Timestamp: {timestamp}")

            for row in rows:
                options_data.append({
                    'strike': float(row[0]),
                    'option_type': row[1],
                    'gamma': float(row[2]) if row[2] else 0,
                    'delta': float(row[3]) if row[3] else 0,
                    'vega': float(row[4]) if row[4] else 0,
                    'open_interest': int(row[5]) if row[5] else 0,
                    'volume': int(row[6]) if row[6] else 0
                })

            cursor.close()

            # Calculate GEX
            logger.info(f"Calculating GEX from {len(options_data)} options...")
            metrics = self._calculate_gex_metrics(
                symbol, expiration, timestamp, underlying_price, options_data
            )

            # Store metrics
            if metrics:
                logger.info(f"✅ GEX calculated: Total=${metrics.total_gamma_exposure/1e6:.1f}M, "
                           f"Net=${metrics.net_gex/1e6:.1f}M, MaxStrike=${metrics.max_gamma_strike:.2f}")
                self._store_gex_metrics(metrics)
            else:
                logger.warning("GEX metrics calculation returned None")

            return metrics

        except Exception as e:
            logger.error(f"Error calculating GEX: {e}", exc_info=True)
            cursor.close()
            return None

    def _calculate_gex_metrics(self, symbol: str, expiration: date, 
                               timestamp: datetime, underlying_price: float,
                               options_data: List[Dict]) -> GEXMetrics:
        """Calculate GEX metrics from options data"""

        # Separate calls and puts
        calls = [opt for opt in options_data if opt['option_type'] == 'call']
        puts = [opt for opt in options_data if opt['option_type'] == 'put']

        # Contract multiplier (100 shares per contract)
        CONTRACT_MULTIPLIER = 100

        # Calculate gamma exposure by strike
        strike_gamma = {}

        # Calls: positive gamma for dealers (they're short)
        for call in calls:
            strike = call['strike']
            gamma_exp = call['gamma'] * call['open_interest'] * CONTRACT_MULTIPLIER * underlying_price

            if strike not in strike_gamma:
                strike_gamma[strike] = {'call': 0, 'put': 0}
            strike_gamma[strike]['call'] += gamma_exp

        # Puts: negative gamma for dealers
        for put in puts:
            strike = put['strike']
            gamma_exp = put['gamma'] * put['open_interest'] * CONTRACT_MULTIPLIER * underlying_price

            if strike not in strike_gamma:
                strike_gamma[strike] = {'call': 0, 'put': 0}
            strike_gamma[strike]['put'] += gamma_exp

        # Aggregate metrics
        total_call_gamma = sum(s['call'] for s in strike_gamma.values())
        total_put_gamma = sum(s['put'] for s in strike_gamma.values())

        # Net GEX (calls are positive, puts are negative for dealers)
        net_gex = total_call_gamma - total_put_gamma

        # Total absolute gamma exposure
        total_gamma = total_call_gamma + total_put_gamma

        # Find max gamma strike
        max_strike = None
        max_gamma = 0

        for strike, gamma in strike_gamma.items():
            total_strike_gamma = gamma['call'] + gamma['put']
            if total_strike_gamma > max_gamma:
                max_gamma = total_strike_gamma
                max_strike = strike

        # Find gamma flip point (where net GEX crosses zero)
        gamma_flip = self._find_gamma_flip(strike_gamma, underlying_price)

        # Volume and OI totals
        call_volume = sum(c['volume'] for c in calls)
        put_volume = sum(p['volume'] for p in puts)
        call_oi = sum(c['open_interest'] for c in calls)
        put_oi = sum(p['open_interest'] for p in puts)

        # Put/Call ratio
        pc_ratio = put_oi / call_oi if call_oi > 0 else 0

        # Vanna and Charm (simplified calculations)
        vanna = sum(opt['vega'] * opt['delta'] * opt['open_interest'] 
                   for opt in options_data)
        charm = sum(opt['gamma'] * opt['delta'] * opt['open_interest'] 
                   for opt in options_data)

        return GEXMetrics(
            symbol=symbol,
            expiration=expiration,
            timestamp=timestamp,
            underlying_price=underlying_price,
            total_gamma_exposure=total_gamma,
            call_gamma=total_call_gamma,
            put_gamma=total_put_gamma,
            net_gex=net_gex,
            call_volume=call_volume,
            put_volume=put_volume,
            call_oi=call_oi,
            put_oi=put_oi,
            total_contracts=call_oi + put_oi,
            max_gamma_strike=max_strike or 0,
            max_gamma_value=max_gamma,
            gamma_flip_point=gamma_flip,
            put_call_ratio=pc_ratio,
            vanna_exposure=vanna,
            charm_exposure=charm
        )

    def _find_gamma_flip(self, strike_gamma: Dict, spot_price: float) -> Optional[float]:
        """Find the strike where net GEX changes sign"""

        strikes = sorted(strike_gamma.keys())

        for i in range(len(strikes) - 1):
            strike1 = strikes[i]
            strike2 = strikes[i + 1]

            net1 = strike_gamma[strike1]['call'] - strike_gamma[strike1]['put']
            net2 = strike_gamma[strike2]['call'] - strike_gamma[strike2]['put']

            # Check for sign change
            if (net1 > 0 and net2 < 0) or (net1 < 0 and net2 > 0):
                # Linear interpolation
                flip = strike1 + (strike2 - strike1) * abs(net1) / (abs(net1) + abs(net2))
                return flip

        return None

    def _store_gex_metrics(self, metrics: GEXMetrics):
        """Store GEX metrics to database"""

        cursor = self.db.cursor()

        insert_query = """
            INSERT INTO gex_metrics 
            (timestamp, symbol, expiration, underlying_price,
             total_gamma_exposure, call_gamma, put_gamma, net_gex,
             call_volume, put_volume, call_oi, put_oi, total_contracts,
             max_gamma_strike, max_gamma_value, gamma_flip_point,
             put_call_ratio, vanna_exposure, charm_exposure)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, symbol, expiration) 
            DO UPDATE SET
                underlying_price = EXCLUDED.underlying_price,
                total_gamma_exposure = EXCLUDED.total_gamma_exposure,
                call_gamma = EXCLUDED.call_gamma,
                put_gamma = EXCLUDED.put_gamma,
                net_gex = EXCLUDED.net_gex,
                max_gamma_strike = EXCLUDED.max_gamma_strike,
                gamma_flip_point = EXCLUDED.gamma_flip_point
        """

        cursor.execute(insert_query, (
            metrics.timestamp,
            metrics.symbol,
            metrics.expiration,
            metrics.underlying_price,
            metrics.total_gamma_exposure,
            metrics.call_gamma,
            metrics.put_gamma,
            metrics.net_gex,
            metrics.call_volume,
            metrics.put_volume,
            metrics.call_oi,
            metrics.put_oi,
            metrics.total_contracts,
            metrics.max_gamma_strike,
            metrics.max_gamma_value,
            metrics.gamma_flip_point,
            metrics.put_call_ratio,
            metrics.vanna_exposure,
            metrics.charm_exposure
        ))

        self.db.commit()
        cursor.close()

        logger.info(f"Stored GEX metrics for {metrics.symbol}")


# Test
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    db = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')
    )

    calc = GEXCalculator(db)
    metrics = calc.calculate_current_gex('SPY')

    if metrics:
        print("\nGEX Metrics:")
        print(f"  Spot: ${metrics.underlying_price:.2f}")
        print(f"  Total GEX: ${metrics.total_gamma_exposure/1e6:.1f}M")
        print(f"  Net GEX: ${metrics.net_gex/1e6:.1f}M")
        print(f"  Max Gamma Strike: ${metrics.max_gamma_strike:.2f}")
        if metrics.gamma_flip_point:
            print(f"  Gamma Flip: ${metrics.gamma_flip_point:.2f}")

    db.close()
