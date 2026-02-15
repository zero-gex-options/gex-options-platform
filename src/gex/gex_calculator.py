"""
Gamma Exposure (GEX) Calculator

Calculates dealer gamma exposure from options market data.
"""

import psycopg2
from datetime import datetime, date, timezone
from typing import Dict, Optional, List, Tuple
from src.gex.gex_metrics import GEXMetrics, StrikeGammaProfile
from src.utils import get_logger

logger = get_logger(__name__)


class GEXCalculator:
    """Calculate gamma exposure metrics from options data"""

    # Contract multiplier (100 shares per contract)
    CONTRACT_MULTIPLIER = 100

    def __init__(self, db_connection):
        """
        Initialize GEX calculator

        Args:
            db_connection: psycopg2 database connection
        """
        logger.debug("Initializing GEXCalculator")
        self.db = db_connection
        logger.info("✅ GEX Calculator initialized")

    def calculate_current_gex(
        self, 
        symbol: str, 
        current_price: Optional[float] = None,
        expiration: Optional[date] = None
    ) -> Optional[GEXMetrics]:
        """
        Calculate GEX for the most recent data

        Args:
            symbol: Underlying symbol (e.g., 'SPY')
            current_price: Optional current price (uses data price if not provided)
            expiration: Optional expiration date (defaults to today for 0DTE)

        Returns:
            GEXMetrics object or None if no data
        """
        logger.info(f"Starting GEX calculation for {symbol}")
        logger.debug(f"Current price: {current_price}, Expiration: {expiration}")

        # Default to today's expiration (0DTE)
        if expiration is None:
            expiration = date.today()
            logger.debug(f"Using today's expiration: {expiration}")

        # Fetch options data
        options_data = self._fetch_options_data(symbol, expiration)

        if not options_data:
            logger.warning(f"No options data for {symbol} exp {expiration}")
            return None

        logger.info(f"Retrieved {len(options_data)} option contracts")

        # Determine underlying price
        if current_price is not None:
            underlying_price = current_price
            logger.info(f"Using provided price: ${underlying_price:.2f}")
        else:
            underlying_price = options_data[0]['underlying_price']
            logger.warning(f"Using price from data: ${underlying_price:.2f}")

        # Calculate GEX metrics
        metrics = self._calculate_gex_metrics(
            symbol, 
            expiration, 
            underlying_price, 
            options_data
        )

        # Store metrics
        if metrics:
            logger.info(f"✅ GEX calculated successfully")
            logger.info(f"   Total GEX: ${metrics.total_gex_millions:.1f}M")
            logger.info(f"   Net GEX: ${metrics.net_gex_millions:.1f}M")
            logger.info(f"   Max Gamma Strike: ${metrics.max_gamma_strike:.2f}")
            if metrics.gamma_flip_point:
                logger.info(f"   Gamma Flip Point: ${metrics.gamma_flip_point:.2f}")

            self._store_gex_metrics(metrics)
        else:
            logger.error("GEX metrics calculation returned None")

        return metrics

    def _fetch_options_data(self, symbol: str, expiration: date) -> List[Dict]:
        """
        Fetch latest options data from database

        Args:
            symbol: Underlying symbol
            expiration: Expiration date

        Returns:
            List of option data dictionaries
        """
        cursor = self.db.cursor()

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
            WHERE symbol LIKE %s
                AND DATE(expiration) = %s
                AND gamma IS NOT NULL
                AND gamma > 0
            ORDER BY strike, option_type, timestamp DESC
        """

        try:
            cursor.execute(query, (f"{symbol}%", expiration))
            rows = cursor.fetchall()

            if not rows:
                logger.debug(f"No data found for {symbol} exp {expiration}")
                # Debug query to see what data exists
                self._log_available_data(symbol)
                return []

            # Parse into list of dicts
            options_data = []
            for row in rows:
                options_data.append({
                    'strike': float(row[0]),
                    'option_type': row[1],
                    'gamma': float(row[2]) if row[2] else 0,
                    'delta': float(row[3]) if row[3] else 0,
                    'vega': float(row[4]) if row[4] else 0,
                    'open_interest': int(row[5]) if row[5] else 0,
                    'volume': int(row[6]) if row[6] else 0,
                    'underlying_price': float(row[7]),
                })

            # Log summary
            calls = sum(1 for opt in options_data if opt['option_type'] == 'call')
            puts = sum(1 for opt in options_data if opt['option_type'] == 'put')
            logger.debug(f"Fetched {calls} calls, {puts} puts")

            return options_data

        except Exception as e:
            logger.error(f"Error fetching options data: {e}", exc_info=True)
            return []
        finally:
            cursor.close()

    def _log_available_data(self, symbol: str):
        """Debug log to show what data exists in database"""
        cursor = self.db.cursor()
        try:
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
            logger.debug(f"Available data - Total: {debug[0]}, "
                        f"Expirations: {debug[1]}, "
                        f"Range: {debug[2]} to {debug[3]}, "
                        f"Latest: {debug[4]}")
        except Exception as e:
            logger.debug(f"Could not fetch debug info: {e}")
        finally:
            cursor.close()

    def _calculate_gex_metrics(
        self,
        symbol: str,
        expiration: date,
        underlying_price: float,
        options_data: List[Dict]
    ) -> Optional[GEXMetrics]:
        """
        Calculate GEX metrics from options data

        Args:
            symbol: Underlying symbol
            expiration: Expiration date
            underlying_price: Current underlying price
            options_data: List of option data dicts

        Returns:
            GEXMetrics object
        """
        logger.debug(f"Calculating GEX for {len(options_data)} options")

        # Separate calls and puts
        calls = [opt for opt in options_data if opt['option_type'] == 'call']
        puts = [opt for opt in options_data if opt['option_type'] == 'put']

        logger.debug(f"Split: {len(calls)} calls, {len(puts)} puts")

        # Calculate gamma exposure by strike
        strike_profiles = self._calculate_strike_profiles(
            calls, puts, underlying_price
        )

        # Aggregate metrics
        total_call_gamma = sum(sp.call_gamma for sp in strike_profiles.values())
        total_put_gamma = sum(sp.put_gamma for sp in strike_profiles.values())

        logger.debug(f"Aggregated - Call: ${total_call_gamma/1e6:.1f}M, "
                    f"Put: ${total_put_gamma/1e6:.1f}M")

        # Net GEX (calls positive, puts negative for dealers)
        net_gex = total_call_gamma - total_put_gamma

        # Total absolute gamma
        total_gamma = total_call_gamma + total_put_gamma

        # Find max gamma strike
        max_strike, max_gamma = self._find_max_gamma_strike(strike_profiles)

        # Find gamma flip point
        gamma_flip = self._find_gamma_flip(strike_profiles, underlying_price)

        # Calculate max pain
        max_pain = self._calculate_max_pain(options_data, underlying_price)
        logger.info(f"Max Pain calculated: ${max_pain:.2f}")

        # Calculate volume and OI totals
        call_volume = sum(c['volume'] for c in calls)
        put_volume = sum(p['volume'] for p in puts)
        call_oi = sum(c['open_interest'] for c in calls)
        put_oi = sum(p['open_interest'] for p in puts)

        # Put/Call ratio
        pc_ratio = put_oi / call_oi if call_oi > 0 else 0

        # Higher order Greeks
        vanna = sum(opt['vega'] * opt['delta'] * opt['open_interest'] 
                   for opt in options_data)
        charm = sum(opt['gamma'] * opt['delta'] * opt['open_interest'] 
                   for opt in options_data)

        # Create metrics object
        metrics = GEXMetrics(
            symbol=symbol,
            expiration=expiration,
            timestamp=datetime.now(timezone.utc),
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
            max_gamma_strike=max_strike,
            max_gamma_value=max_gamma,
            gamma_flip_point=gamma_flip,
            max_pain=max_pain,
            put_call_ratio=pc_ratio,
            vanna_exposure=vanna,
            charm_exposure=charm,
            strike_profiles=strike_profiles
        )

        logger.debug("GEX metrics object created")
        return metrics

    def _calculate_strike_profiles(
        self,
        calls: List[Dict],
        puts: List[Dict],
        underlying_price: float
    ) -> Dict[float, StrikeGammaProfile]:
        """
        Calculate gamma profiles for each strike

        Args:
            calls: List of call options
            puts: List of put options
            underlying_price: Current price

        Returns:
            Dictionary mapping strike to StrikeGammaProfile
        """
        profiles = {}

        # Process calls
        for call in calls:
            strike = call['strike']
            gamma_exp = (call['gamma'] * call['open_interest'] * 
                        self.CONTRACT_MULTIPLIER * underlying_price)

            if strike not in profiles:
                profiles[strike] = StrikeGammaProfile(
                    strike=strike,
                    call_gamma=0,
                    put_gamma=0,
                    net_gamma=0,
                    total_gamma=0,
                    call_oi=0,
                    put_oi=0,
                    call_volume=0,
                    put_volume=0
                )

            profiles[strike].call_gamma += gamma_exp
            profiles[strike].call_oi += call['open_interest']
            profiles[strike].call_volume += call['volume']

        # Process puts
        for put in puts:
            strike = put['strike']
            gamma_exp = (put['gamma'] * put['open_interest'] * 
                        self.CONTRACT_MULTIPLIER * underlying_price)

            if strike not in profiles:
                profiles[strike] = StrikeGammaProfile(
                    strike=strike,
                    call_gamma=0,
                    put_gamma=0,
                    net_gamma=0,
                    total_gamma=0,
                    call_oi=0,
                    put_oi=0,
                    call_volume=0,
                    put_volume=0
                )

            profiles[strike].put_gamma += gamma_exp
            profiles[strike].put_oi += put['open_interest']
            profiles[strike].put_volume += put['volume']

        # Calculate net and total for each strike
        for profile in profiles.values():
            profile.net_gamma = profile.call_gamma - profile.put_gamma
            profile.total_gamma = profile.call_gamma + profile.put_gamma

        logger.debug(f"Created profiles for {len(profiles)} strikes")
        return profiles

    def _find_max_gamma_strike(
        self, 
        strike_profiles: Dict[float, StrikeGammaProfile]
    ) -> Tuple[float, float]:
        """Find strike with maximum total gamma"""
        if not strike_profiles:
            return 0.0, 0.0

        max_strike = max(strike_profiles.items(), 
                        key=lambda x: x[1].total_gamma)

        logger.debug(f"Max gamma strike: ${max_strike[0]:.2f} "
                    f"with ${max_strike[1].gamma_exposure_millions:.1f}M")

        return max_strike[0], max_strike[1].total_gamma

    def _find_gamma_flip(
        self,
        strike_profiles: Dict[float, StrikeGammaProfile],
        spot_price: float
    ) -> Optional[float]:
        """
        Find strike where net GEX changes sign (zero gamma point)

        Args:
            strike_profiles: Strike gamma profiles
            spot_price: Current spot price

        Returns:
            Gamma flip point or None
        """
        strikes = sorted(strike_profiles.keys())

        if len(strikes) < 2:
            return None

        for i in range(len(strikes) - 1):
            strike1 = strikes[i]
            strike2 = strikes[i + 1]

            net1 = strike_profiles[strike1].net_gamma
            net2 = strike_profiles[strike2].net_gamma

            # Check for sign change
            if (net1 > 0 and net2 < 0) or (net1 < 0 and net2 > 0):
                # Linear interpolation to find exact crossing point
                flip = strike1 + (strike2 - strike1) * abs(net1) / (abs(net1) + abs(net2))
                logger.debug(f"Gamma flip between ${strike1:.2f} and ${strike2:.2f} "
                           f"at ${flip:.2f}")
                return flip

        logger.debug("No gamma flip point found")
        return None

    def _calculate_max_pain(
        self,
        options_data: List[Dict],
        current_price: float
    ) -> float:
        """
        Calculate max pain - the strike price where the most option value expires worthless.

        Args:
            options_data: List of option data dictionaries
            current_price: Current underlying price

        Returns:
            Max pain strike price
        """
        if not options_data:
            logger.debug("No options data for max pain calculation")
            return current_price

        # Get all unique strikes
        strikes = sorted(set(opt['strike'] for opt in options_data))

        logger.debug(f"Calculating max pain across {len(strikes)} strikes")

        # For each strike, calculate total value retained by option holders at that closing price
        max_pain_strike = current_price
        min_total_value = float('inf')

        for strike in strikes:
            total_value = 0

            for opt in options_data:
                opt_strike = opt['strike']
                oi = opt.get('open_interest', 0)
                opt_type = opt['option_type']

                if oi == 0:
                    continue

                # Calculate intrinsic value at this strike price
                if opt_type == 'call':
                    # Call value = max(0, closing_price - strike)
                    intrinsic = max(0, strike - opt_strike)
                else:  # put
                    # Put value = max(0, strike - closing_price)
                    intrinsic = max(0, opt_strike - strike)

                # Total value retained by option holders at this closing price
                # Value = intrinsic * OI * 100 (contract multiplier)
                total_value += intrinsic * oi * self.CONTRACT_MULTIPLIER

            # Max pain is where total value is minimized (most value destroyed)
            if total_value < min_total_value:
                min_total_value = total_value
                max_pain_strike = strike

        logger.debug(f"Max pain strike: ${max_pain_strike:.2f} "
                    f"(min option value: ${min_total_value/1e6:.1f}M)")

        return max_pain_strike

    def _store_gex_metrics(self, metrics: GEXMetrics):
        """
        Store GEX metrics to database

        Args:
            metrics: GEXMetrics object to store
        """
        logger.debug(f"Storing GEX metrics for {metrics.symbol}")

        cursor = self.db.cursor()

        insert_query = """
            INSERT INTO gex_metrics
            (timestamp, symbol, expiration, underlying_price,
             total_gamma_exposure, call_gamma, put_gamma, net_gex,
             call_volume, put_volume, call_oi, put_oi, total_contracts,
             max_gamma_strike, max_gamma_value, gamma_flip_point, max_pain,
             put_call_ratio, vanna_exposure, charm_exposure)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, symbol, expiration)
            DO UPDATE SET
                underlying_price = EXCLUDED.underlying_price,
                total_gamma_exposure = EXCLUDED.total_gamma_exposure,
                call_gamma = EXCLUDED.call_gamma,
                put_gamma = EXCLUDED.put_gamma,
                net_gex = EXCLUDED.net_gex,
                max_gamma_strike = EXCLUDED.max_gamma_strike,
                gamma_flip_point = EXCLUDED.gamma_flip_point,
                max_pain = EXCLUDED.max_pain
        """

        try:
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
                metrics.max_pain,
                metrics.put_call_ratio,
                metrics.vanna_exposure,
                metrics.charm_exposure
            ))

            self.db.commit()
            logger.info(f"✅ GEX metrics stored for {metrics.symbol}")
            logger.debug(f"   Timestamp: {metrics.timestamp}")

        except Exception as e:
            logger.error(f"Failed to store GEX metrics: {e}", exc_info=True)
            self.db.rollback()
            raise
        finally:
            cursor.close()


# Test/Demo
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logger.info("Testing GEX Calculator...")

    try:
        # Connect to database
        db = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )

        logger.info("✅ Database connected")

        # Create calculator
        calc = GEXCalculator(db)

        # Calculate GEX
        metrics = calc.calculate_current_gex('SPY')

        if metrics:
            print("\n" + "="*60)
            print(metrics.summary())
            print("="*60)
        else:
            logger.error("No metrics calculated")

        db.close()

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
