"""
Full pipeline integration test

Tests the complete flow:
1. TradeStation streaming → 2. Database storage → 3. GEX calculation
"""

import asyncio
import psycopg2
import os
import sys
from dotenv import load_dotenv
from datetime import date, datetime
import logging

# Add src directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'ingestion'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'gex'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


async def test_full_pipeline():
    """Test the complete data pipeline"""
    
    logger.info("="*80)
    logger.info("FULL PIPELINE INTEGRATION TEST")
    logger.info("="*80)
    
    # Connect to database
    db = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')
    )
    
    cursor = db.cursor()
    
    # Test 1: Check database tables exist
    logger.info("\nTest 1: Database Schema")
    logger.info("-" * 40)
    
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    
    tables = [row[0] for row in cursor.fetchall()]
    logger.info(f"Found {len(tables)} tables:")
    for table in tables:
        logger.info(f"  ✓ {table}")
    
    required_tables = ['options_quotes', 'underlying_prices', 'gex_metrics', 'ingestion_metrics']
    schema_ok = all(t in tables for t in required_tables)
    
    for table in required_tables:
        if table in tables:
            logger.info(f"  ✅ {table} exists")
        else:
            logger.error(f"  ❌ {table} MISSING!")
    
    # Test 2: Run ingestion for 30 seconds
    logger.info("\nTest 2: Data Ingestion (30 seconds)")
    logger.info("-" * 40)
    
    from tradestation_streaming_ingestion_engine import StreamingIngestionEngine
    
    engine = StreamingIngestionEngine()
    
    # Start ingestion in background
    ingestion_task = asyncio.create_task(engine.run('SPY'))
    
    # Let it run for 30 seconds
    logger.info("Running ingestion for 30 seconds...")
    await asyncio.sleep(30)
    
    # Stop ingestion
    ingestion_task.cancel()
    try:
        await ingestion_task
    except asyncio.CancelledError:
        pass
    
    logger.info(f"Ingestion stats:")
    logger.info(f"  Options received: {engine.options_received}")
    logger.info(f"  Options stored: {engine.options_stored}")
    logger.info(f"  Errors: {engine.errors}")
    
    # Test 3: Verify data in database
    logger.info("\nTest 3: Verify Stored Data")
    logger.info("-" * 40)
    
    # Check options_quotes
    cursor.execute("""
        SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
        FROM options_quotes
        WHERE timestamp > NOW() - INTERVAL '1 hour'
    """)
    
    count, min_ts, max_ts = cursor.fetchone()
    logger.info(f"Options quotes in last hour:")
    logger.info(f"  Records: {count}")
    logger.info(f"  Oldest: {min_ts}")
    logger.info(f"  Newest: {max_ts}")
    
    data_ok = count > 0
    
    if count > 0:
        logger.info(f"  ✅ Data successfully stored")
        
        # Show sample data
        cursor.execute("""
            SELECT symbol, strike, option_type, bid, ask, volume, open_interest, delta, gamma
            FROM options_quotes
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            LIMIT 5
        """)
        
        logger.info("\nSample options data:")
        for row in cursor.fetchall():
            symbol, strike, opt_type, bid, ask, vol, oi, delta, gamma = row
            logger.info(f"  {symbol} {opt_type.upper()} ${strike:.2f} "
                       f"bid={bid:.2f} ask={ask:.2f} vol={vol} OI={oi} "
                       f"delta={delta:.3f} gamma={gamma:.6f}")
    else:
        logger.warning(f"  ⚠️  No data stored yet")
    
    # Check underlying_prices
    cursor.execute("""
        SELECT COUNT(*), AVG(price)
        FROM underlying_prices
        WHERE timestamp > NOW() - INTERVAL '1 hour'
    """)
    
    und_count, avg_price = cursor.fetchone()
    logger.info(f"\nUnderlying prices:")
    logger.info(f"  Records: {und_count}")
    logger.info(f"  Avg SPY price: ${avg_price:.2f}" if avg_price else "  No data")
    
    # Test 4: Run GEX calculation
    logger.info("\nTest 4: GEX Calculation")
    logger.info("-" * 40)
    
    gex_ok = False
    
    if count > 0:
        from gex_calculator import GEXCalculator
        
        calculator = GEXCalculator(db)
        
        try:
            metrics = calculator.calculate_current_gex('SPY')
            
            if metrics:
                logger.info("GEX Metrics calculated successfully:")
                logger.info(f"  Symbol: {metrics.symbol}")
                logger.info(f"  Expiration: {metrics.expiration}")
                logger.info(f"  Spot: ${metrics.underlying_price:.2f}")
                logger.info(f"  Total GEX: ${metrics.total_gamma_exposure/1e6:.1f}M")
                logger.info(f"  Call Gamma: ${metrics.call_gamma/1e6:.1f}M")
                logger.info(f"  Put Gamma: ${metrics.put_gamma/1e6:.1f}M")
                logger.info(f"  Net GEX: ${metrics.net_gex/1e6:.1f}M")
                logger.info(f"  Max Gamma Strike: ${metrics.max_gamma_strike:.2f}")
                if metrics.gamma_flip_point:
                    logger.info(f"  Gamma Flip: ${metrics.gamma_flip_point:.2f}")
                logger.info(f"  ✅ GEX calculation working")
                gex_ok = True
            else:
                logger.warning(f"  ⚠️  No metrics calculated")
                
        except Exception as e:
            logger.error(f"  ❌ GEX calculation failed: {e}")
    else:
        logger.info("  ⏭️  Skipping (no data to calculate from)")
        gex_ok = True  # Don't fail test if no data yet
    
    # Test 5: Check GEX metrics storage
    logger.info("\nTest 5: GEX Metrics Storage")
    logger.info("-" * 40)
    
    cursor.execute("""
        SELECT COUNT(*), MAX(timestamp)
        FROM gex_metrics
        WHERE timestamp > NOW() - INTERVAL '1 hour'
    """)
    
    gex_count, latest = cursor.fetchone()
    logger.info(f"GEX metrics records: {gex_count}")
    logger.info(f"Latest: {latest}")
    
    if gex_count > 0:
        logger.info(f"  ✅ GEX metrics stored")
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("TEST SUMMARY")
    logger.info("="*80)
    
    all_passed = True
    
    checks = [
        ("Database schema", schema_ok),
        ("Data ingestion", engine.options_received > 0),
        ("Data storage", data_ok),
        ("GEX calculation", gex_ok),
    ]
    
    for check_name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{status}: {check_name}")
        if not passed:
            all_passed = False
    
    if all_passed:
        logger.info("\n🎉 ALL TESTS PASSED - Pipeline is working!")
    else:
        logger.warning("\n⚠️  Some tests failed - review logs above")
    
    cursor.close()
    db.close()
    
    return all_passed


if __name__ == '__main__':
    try:
        result = asyncio.run(test_full_pipeline())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(1)
