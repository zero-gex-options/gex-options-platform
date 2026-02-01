#!/usr/bin/env python3
"""
GEX Command Line Interface

Utility for analyzing GEX metrics from the command line.
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import date
from dotenv import load_dotenv
import psycopg2

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.gex.gex_calculator import GEXCalculator
from src.gex.gex_analyzer import GEXAnalyzer
from src.utils import get_logger

# Load environment
env_file = project_root / ".env"
load_dotenv(env_file)

logger = get_logger(__name__)


def load_db_credentials() -> dict:
    """Load database credentials"""
    creds_file = Path.home() / ".zerogex_db_creds"
    
    if not creds_file.exists():
        raise FileNotFoundError(f"Database credentials not found: {creds_file}")
    
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


def cmd_calculate(args):
    """Calculate GEX for a symbol"""
    print(f"\n{'='*60}")
    print(f"Calculating GEX for {args.symbol}")
    print(f"{'='*60}\n")
    
    # Connect to database
    db_creds = load_db_credentials()
    db = psycopg2.connect(**db_creds)
    
    # Create calculator
    calc = GEXCalculator(db)
    
    # Calculate
    expiration = date.today() if args.expiration == 'today' else date.fromisoformat(args.expiration)
    metrics = calc.calculate_current_gex(args.symbol, expiration=expiration)
    
    if metrics:
        print(metrics.summary())
    else:
        print("❌ No GEX metrics calculated")
    
    db.close()


def cmd_summary(args):
    """Show summary of current GEX state"""
    db_creds = load_db_credentials()
    db = psycopg2.connect(**db_creds)
    
    analyzer = GEXAnalyzer(db)
    summary = analyzer.summarize_current_state(args.symbol)
    print(summary)
    
    db.close()


def cmd_levels(args):
    """Find key gamma levels"""
    db_creds = load_db_credentials()
    db = psycopg2.connect(**db_creds)
    
    analyzer = GEXAnalyzer(db)
    levels = analyzer.find_key_gamma_levels(args.symbol, args.threshold)
    
    print(f"\n{'='*60}")
    print(f"KEY GAMMA LEVELS: {args.symbol}")
    print(f"Threshold: ${args.threshold}M")
    print(f"{'='*60}\n")
    
    if levels['support']:
        print(f"SUPPORT LEVELS (High Put Gamma):")
        for strike in levels['support'][:10]:
            print(f"  ${strike:.2f}")
        print()
    else:
        print("No significant support levels found\n")
    
    if levels['resistance']:
        print(f"RESISTANCE LEVELS (High Call Gamma):")
        for strike in levels['resistance'][:10]:
            print(f"  ${strike:.2f}")
        print()
    else:
        print("No significant resistance levels found\n")
    
    print(f"{'='*60}\n")
    
    db.close()


def cmd_regime(args):
    """Analyze gamma regime changes"""
    db_creds = load_db_credentials()
    db = psycopg2.connect(**db_creds)
    
    analyzer = GEXAnalyzer(db)
    changes = analyzer.analyze_gamma_regime_changes(args.symbol, args.hours)
    
    print(f"\n{'='*60}")
    print(f"GAMMA REGIME CHANGES: {args.symbol}")
    print(f"Period: Last {args.hours} hours")
    print(f"{'='*60}\n")
    
    if changes:
        for change in changes:
            timestamp = change['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            print(f"{timestamp}:")
            print(f"  {change['from_regime'].upper()} → {change['to_regime'].upper()}")
            print(f"  Price: ${change['price']:.2f}")
            print(f"  Net GEX: ${change['net_gex']/1e6:+.1f}M")
            print()
    else:
        print("No regime changes detected in this period\n")
    
    print(f"{'='*60}\n")
    
    db.close()


def cmd_expected_move(args):
    """Calculate expected price move"""
    db_creds = load_db_credentials()
    db = psycopg2.connect(**db_creds)
    
    analyzer = GEXAnalyzer(db)
    move = analyzer.calculate_expected_move(args.symbol, args.confidence)
    
    print(f"\n{'='*60}")
    print(f"EXPECTED MOVE: {args.symbol}")
    print(f"Confidence: {args.confidence*100:.0f}%")
    print(f"{'='*60}\n")
    
    if move:
        print(f"Current Price: ${move['spot_price']:.2f}")
        print(f"Expected Range: ${move['expected_low']:.2f} - ${move['expected_high']:.2f}")
        print(f"Move: ±{move['move_pct']:.1f}%")
        print(f"Max Gamma Strike: ${move['max_gamma_strike']:.2f}")
    else:
        print("Unable to calculate expected move")
    
    print(f"\n{'='*60}\n")
    
    db.close()


def cmd_history(args):
    """Show historical GEX metrics"""
    db_creds = load_db_credentials()
    db = psycopg2.connect(**db_creds)
    
    analyzer = GEXAnalyzer(db)
    metrics = analyzer.get_historical_metrics(args.symbol, args.hours)
    
    print(f"\n{'='*60}")
    print(f"HISTORICAL GEX: {args.symbol}")
    print(f"Period: Last {args.hours} hours")
    print(f"{'='*60}\n")
    
    if metrics:
        print(f"{'Timestamp':<20} {'Price':>8} {'Total GEX':>12} {'Net GEX':>12} {'Max Gamma':>10}")
        print(f"{'-'*70}")
        
        for m in metrics[-20:]:  # Show last 20
            timestamp = m['timestamp'].strftime('%m/%d %H:%M')
            price = f"${m['underlying_price']:.2f}"
            total_gex = f"${m['total_gamma_exposure']/1e6:.1f}M"
            net_gex = f"${m['net_gex']/1e6:+.1f}M"
            max_gamma = f"${m['max_gamma_strike']:.2f}"
            
            print(f"{timestamp:<20} {price:>8} {total_gex:>12} {net_gex:>12} {max_gamma:>10}")
    else:
        print("No historical data available")
    
    print(f"\n{'='*60}\n")
    
    db.close()


def main():
    parser = argparse.ArgumentParser(
        description='GEX Analysis Command Line Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s calculate SPY
  %(prog)s summary SPY
  %(prog)s levels SPY --threshold 100
  %(prog)s regime SPY --hours 48
  %(prog)s expected-move SPY --confidence 0.95
  %(prog)s history SPY --hours 24
        '''
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Calculate command
    calc_parser = subparsers.add_parser('calculate', help='Calculate GEX')
    calc_parser.add_argument('symbol', help='Symbol to calculate')
    calc_parser.add_argument('--expiration', default='today', help='Expiration date (today or YYYY-MM-DD)')
    calc_parser.set_defaults(func=cmd_calculate)
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', help='Show GEX summary')
    summary_parser.add_argument('symbol', help='Symbol to summarize')
    summary_parser.set_defaults(func=cmd_summary)
    
    # Levels command
    levels_parser = subparsers.add_parser('levels', help='Find key gamma levels')
    levels_parser.add_argument('symbol', help='Symbol to analyze')
    levels_parser.add_argument('--threshold', type=float, default=50.0, 
                              help='Minimum gamma in millions (default: 50)')
    levels_parser.set_defaults(func=cmd_levels)
    
    # Regime command
    regime_parser = subparsers.add_parser('regime', help='Analyze regime changes')
    regime_parser.add_argument('symbol', help='Symbol to analyze')
    regime_parser.add_argument('--hours', type=int, default=24, 
                               help='Hours to analyze (default: 24)')
    regime_parser.set_defaults(func=cmd_regime)
    
    # Expected move command
    move_parser = subparsers.add_parser('expected-move', help='Calculate expected move')
    move_parser.add_argument('symbol', help='Symbol to analyze')
    move_parser.add_argument('--confidence', type=float, default=0.68,
                            help='Confidence level (0.68, 0.95, etc.)')
    move_parser.set_defaults(func=cmd_expected_move)
    
    # History command
    history_parser = subparsers.add_parser('history', help='Show historical metrics')
    history_parser.add_argument('symbol', help='Symbol to query')
    history_parser.add_argument('--hours', type=int, default=24,
                                help='Hours of history (default: 24)')
    history_parser.set_defaults(func=cmd_history)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        args.func(args)
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        logger.error(f"Command failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
