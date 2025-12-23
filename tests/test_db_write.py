import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, date

load_dotenv('/home/ubuntu/gex-options-platform/.env')

print("Connecting to database...")
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    port=os.getenv('DB_PORT')
)

print("✅ Connected")

cursor = conn.cursor()

print("Inserting test record...")
cursor.execute("""
    INSERT INTO options_quotes 
    (timestamp, symbol, underlying_price, strike, expiration, dte,
     option_type, bid, ask, mid, last, volume, open_interest,
     implied_vol, delta, gamma, theta, vega, rho,
     is_calculated, spread_pct, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (
    datetime.now(), 'SPY', 684.0, 685.0, date.today(), 0,
    'call', 1.0, 1.1, 1.05, 1.05, 100, 500,
    0.15, 0.5, 0.01, -0.05, 0.02, 0.01,
    False, 0.1, 'test'
))

conn.commit()
print("✅ Committed")

cursor.execute("SELECT COUNT(*) FROM options_quotes WHERE source = 'test'")
count = cursor.fetchone()[0]
print(f"✅ Test records in database: {count}")

cursor.close()
conn.close()

print("✅ Test successful - database writes work!")
