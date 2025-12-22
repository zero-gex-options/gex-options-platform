from datetime import datetime, time as dt_time, timezone
import pytz

et_tz = pytz.timezone('America/New_York')
utc_tz = timezone.utc

now_utc = datetime.now(utc_tz)
now_et = datetime.now(et_tz)

print("="*60)
print("MARKET HOURS DEBUG")
print("="*60)
print(f"Server time (UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"Current ET time: {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"Day of week: {now_et.weekday()} (0=Mon, 4=Fri, 5=Sat, 6=Sun)")
print(f"Current time: {now_et.time()}")
print()

market_open = dt_time(9, 30)
market_close = dt_time(16, 0)
current_time = now_et.time()

print(f"Market opens: {market_open}")
print(f"Market closes: {market_close}")
print()

is_weekday = now_et.weekday() < 5
is_open_hours = market_open <= current_time <= market_close

print(f"Is weekday: {is_weekday}")
print(f"In market hours: {is_open_hours}")
print(f"Market is OPEN: {is_weekday and is_open_hours}")
print("="*60)
