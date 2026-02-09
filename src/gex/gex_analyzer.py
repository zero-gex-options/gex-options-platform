"""
GEX Analyzer

Advanced analysis and insights from GEX metrics.
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import psycopg2
from src.gex.gex_metrics import GEXMetrics, StrikeGammaProfile
from src.utils import get_logger

logger = get_logger(__name__)


class GEXAnalyzer:
    """Analyze GEX metrics for trading insights"""
    
    def __init__(self, db_connection):
        """
        Initialize analyzer
        
        Args:
            db_connection: psycopg2 database connection
        """
        self.db = db_connection
        logger.info("✅ GEX Analyzer initialized")
    
    def get_historical_metrics(
        self,
        symbol: str,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get historical GEX metrics
        
        Args:
            symbol: Symbol to query
            hours: Hours of history to retrieve
        
        Returns:
            List of GEX metric dictionaries
        """
        cursor = self.db.cursor()
        
        query = """
            SELECT 
                timestamp,
                underlying_price,
                total_gamma_exposure,
                net_gex,
                max_gamma_strike,
                gamma_flip_point,
                put_call_ratio
            FROM gex_metrics
            WHERE symbol = %s
                AND timestamp > NOW() - INTERVAL '%s hours'
            ORDER BY timestamp ASC
        """
        
        try:
            cursor.execute(query, (symbol, hours))
            rows = cursor.fetchall()
            
            metrics = []
            for row in rows:
                metrics.append({
                    'timestamp': row[0],
                    'underlying_price': float(row[1]),
                    'total_gamma_exposure': float(row[2]),
                    'net_gex': float(row[3]),
                    'max_gamma_strike': float(row[4]),
                    'gamma_flip_point': float(row[5]) if row[5] else None,
                    'put_call_ratio': float(row[6])
                })
            
            logger.info(f"Retrieved {len(metrics)} historical metrics for {symbol}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error fetching historical metrics: {e}", exc_info=True)
            return []
        finally:
            cursor.close()
    
    def analyze_gamma_regime_changes(
        self,
        symbol: str,
        hours: int = 24
    ) -> List[Dict]:
        """
        Identify gamma regime changes (positive/negative transitions)
        
        Args:
            symbol: Symbol to analyze
            hours: Hours to analyze
        
        Returns:
            List of regime change events
        """
        metrics = self.get_historical_metrics(symbol, hours)
        
        if len(metrics) < 2:
            logger.warning("Insufficient data for regime analysis")
            return []
        
        changes = []
        prev_regime = 'positive' if metrics[0]['net_gex'] > 0 else 'negative'
        
        for i in range(1, len(metrics)):
            current_gex = metrics[i]['net_gex']
            current_regime = 'positive' if current_gex > 0 else 'negative'
            
            if current_regime != prev_regime:
                changes.append({
                    'timestamp': metrics[i]['timestamp'],
                    'from_regime': prev_regime,
                    'to_regime': current_regime,
                    'price': metrics[i]['underlying_price'],
                    'net_gex': current_gex
                })
                
                logger.info(f"Regime change at {metrics[i]['timestamp']}: "
                          f"{prev_regime} → {current_regime}")
            
            prev_regime = current_regime
        
        return changes
    
    def find_key_gamma_levels(
        self,
        symbol: str,
        threshold_millions: float = 50.0
    ) -> Dict[str, List[float]]:
        """
        Find strikes with significant gamma exposure
        
        Args:
            symbol: Symbol to analyze
            threshold_millions: Minimum gamma exposure in millions
        
        Returns:
            Dictionary with 'support' and 'resistance' strike lists
        """
        cursor = self.db.cursor()
        
        # Get latest options data with gamma
        query = """
            SELECT DISTINCT ON (strike, option_type)
                strike,
                option_type,
                gamma,
                open_interest,
                underlying_price
            FROM options_quotes
            WHERE symbol LIKE %s
                AND timestamp > NOW() - INTERVAL '30 minutes'
                AND gamma IS NOT NULL
            ORDER BY strike, option_type, timestamp DESC
        """
        
        try:
            cursor.execute(query, (f"{symbol}%",))
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning("No recent options data for key level analysis")
                return {'support': [], 'resistance': []}
            
            # Calculate gamma by strike
            strike_gamma = {}
            spot_price = rows[0][4]
            
            for row in rows:
                strike = float(row[0])
                opt_type = row[1]
                gamma = float(row[2])
                oi = int(row[3])
                
                gamma_exp = gamma * oi * 100 * spot_price / 1e6  # In millions
                
                if strike not in strike_gamma:
                    strike_gamma[strike] = {'call': 0, 'put': 0}
                
                if opt_type == 'call':
                    strike_gamma[strike]['call'] += gamma_exp
                else:
                    strike_gamma[strike]['put'] += gamma_exp
            
            # Find significant levels
            support_levels = []  # High put gamma (dealers buy on dips)
            resistance_levels = []  # High call gamma (dealers sell on rallies)
            
            for strike, gamma in strike_gamma.items():
                if gamma['put'] >= threshold_millions and strike <= spot_price:
                    support_levels.append(strike)
                
                if gamma['call'] >= threshold_millions and strike >= spot_price:
                    resistance_levels.append(strike)
            
            logger.info(f"Found {len(support_levels)} support and "
                       f"{len(resistance_levels)} resistance levels")
            
            return {
                'support': sorted(support_levels, reverse=True),
                'resistance': sorted(resistance_levels)
            }
            
        except Exception as e:
            logger.error(f"Error finding key levels: {e}", exc_info=True)
            return {'support': [], 'resistance': []}
        finally:
            cursor.close()
    
    def calculate_expected_move(
        self,
        symbol: str,
        confidence: float = 0.68
    ) -> Optional[Dict]:
        """
        Calculate expected price move based on GEX
        
        Args:
            symbol: Symbol to analyze
            confidence: Confidence level (0.68 = 1 stdev, 0.95 = 2 stdev)
        
        Returns:
            Dictionary with expected move range
        """
        cursor = self.db.cursor()
        
        try:
            # Get latest metrics
            cursor.execute("""
                SELECT 
                    underlying_price,
                    total_gamma_exposure,
                    max_gamma_strike
                FROM gex_metrics
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            
            row = cursor.fetchone()
            
            if not row:
                logger.warning("No recent GEX metrics for expected move calculation")
                return None
            
            spot = float(row[0])
            total_gex = float(row[1])
            max_gamma = float(row[2])
            
            # Simplified expected move based on max gamma strike
            # In reality, this would use more sophisticated models
            distance_to_max_gamma = abs(spot - max_gamma)
            
            # Estimate move based on confidence level
            if confidence == 0.68:  # 1 standard deviation
                move_pct = (distance_to_max_gamma / spot) * 0.68
            elif confidence == 0.95:  # 2 standard deviations
                move_pct = (distance_to_max_gamma / spot) * 0.95
            else:
                move_pct = (distance_to_max_gamma / spot) * confidence
            
            upper = spot * (1 + move_pct)
            lower = spot * (1 - move_pct)
            
            result = {
                'spot_price': spot,
                'expected_high': upper,
                'expected_low': lower,
                'move_pct': move_pct * 100,
                'confidence': confidence,
                'max_gamma_strike': max_gamma
            }
            
            logger.info(f"Expected move: ${lower:.2f} - ${upper:.2f} "
                       f"({move_pct*100:.1f}% at {confidence*100:.0f}% confidence)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating expected move: {e}", exc_info=True)
            return None
        finally:
            cursor.close()
    
    def summarize_current_state(self, symbol: str) -> str:
        """
        Generate human-readable summary of current GEX state
        
        Args:
            symbol: Symbol to summarize
        
        Returns:
            Formatted summary string
        """
        cursor = self.db.cursor()
        
        try:
            # Get latest metrics
            cursor.execute("""
                SELECT 
                    timestamp,
                    underlying_price,
                    total_gamma_exposure,
                    net_gex,
                    max_gamma_strike,
                    gamma_flip_point,
                    put_call_ratio,
                    call_oi,
                    put_oi
                FROM gex_metrics
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            
            row = cursor.fetchone()
            
            if not row:
                return f"No GEX data available for {symbol}"
            
            # Parse data
            timestamp = row[0]
            spot = float(row[1])
            total_gex = float(row[2]) / 1e6
            net_gex = float(row[3]) / 1e6
            max_gamma = float(row[4])
            flip = float(row[5]) if row[5] else None
            pc_ratio = float(row[6])
            call_oi = int(row[7])
            put_oi = int(row[8])
            
            # Determine regime
            regime = "Positive (Stabilizing)" if net_gex > 0 else "Negative (Destabilizing)"
            
            # Build summary
            lines = [
                f"\n{'='*60}",
                f"GEX SUMMARY: {symbol}",
                f"{'='*60}",
                f"Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
                f"",
                f"CURRENT STATE:",
                f"  Spot Price: ${spot:.2f}",
                f"  Gamma Regime: {regime}",
                f"  ",
                f"GAMMA EXPOSURE:",
                f"  Total GEX: ${total_gex:.1f}M",
                f"  Net GEX: ${net_gex:+.1f}M",
                f"  ",
                f"KEY LEVELS:",
                f"  Max Gamma Strike: ${max_gamma:.2f}",
            ]
            
            if flip:
                lines.append(f"  Gamma Flip Point: ${flip:.2f}")
                if spot < flip:
                    lines.append(f"  → Price below flip (bearish bias)")
                else:
                    lines.append(f"  → Price above flip (bullish bias)")
            
            lines.extend([
                f"  ",
                f"POSITIONING:",
                f"  Call OI: {call_oi:,}",
                f"  Put OI: {put_oi:,}",
                f"  Put/Call Ratio: {pc_ratio:.2f}",
                f"{'='*60}\n"
            ])
            
            return "\n".join(lines)
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}", exc_info=True)
            return f"Error generating summary for {symbol}"
        finally:
            cursor.close()


# Test/Demo
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    logger.info("Testing GEX Analyzer...")
    
    try:
        db = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        analyzer = GEXAnalyzer(db)
        
        # Test summary
        summary = analyzer.summarize_current_state('SPY')
        print(summary)
        
        # Test key levels
        levels = analyzer.find_key_gamma_levels('SPY', threshold_millions=50)
        print("\nKEY GAMMA LEVELS:")
        print(f"  Support: {levels['support'][:5]}")
        print(f"  Resistance: {levels['resistance'][:5]}")
        
        # Test expected move
        move = analyzer.calculate_expected_move('SPY')
        if move:
            print(f"\nEXPECTED MOVE:")
            print(f"  Range: ${move['expected_low']:.2f} - ${move['expected_high']:.2f}")
            print(f"  Move: ±{move['move_pct']:.1f}%")
        
        db.close()
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
