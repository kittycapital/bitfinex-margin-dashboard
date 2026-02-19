#!/usr/bin/env python3
"""
Bitfinex Margin Long/Short Position Data Collector
Collects BTC, ETH, SOL margin longs/shorts and price data.
Designed to run via GitHub Actions on a schedule.
"""

import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api-pub.bitfinex.com/v2"
DATA_DIR = "data"

COINS = {
    "btc": "tBTCUSD",
    "eth": "tETHUSD",
    "sol": "tSOLUSD",
}

# Period configs: (label, days_back, candle_timeframe)
PERIODS = {
    "90d":  (90,    "1h"),
    "1y":   (365,   "4h"),
    "3y":   (1095,  "1D"),
    "5y":   (1825,  "1D"),
    "all":  (3650,  "1D"),  # ~10 years, will get whatever is available
}

RATE_LIMIT_DELAY = 2.5  # seconds between API calls


def fetch_json(url, retries=3):
    """Fetch JSON from URL with retry logic."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "HerdVibe-Collector/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(5)
    return []


def fetch_position_data(symbol, side, start_ms, limit=10000):
    """Fetch margin position (long/short) historical data."""
    url = (
        f"{BASE_URL}/stats1/pos.size:1m:{symbol}:{side}/hist"
        f"?limit={limit}&start={start_ms}&sort=1"
    )
    print(f"  Fetching {symbol} {side}...")
    data = fetch_json(url)
    time.sleep(RATE_LIMIT_DELAY)
    return data  # [[timestamp_ms, amount], ...]


def fetch_candle_data(symbol, timeframe, start_ms, limit=10000):
    """Fetch OHLCV candle data for price."""
    url = (
        f"{BASE_URL}/candles/trade:{timeframe}:{symbol}/hist"
        f"?limit={limit}&start={start_ms}&sort=1"
    )
    print(f"  Fetching {symbol} candles ({timeframe})...")
    data = fetch_json(url)
    time.sleep(RATE_LIMIT_DELAY)
    # Returns [[MTS, OPEN, CLOSE, HIGH, LOW, VOLUME], ...]
    return data


def paginate_fetch(fetch_func, *args, max_pages=5, limit_per_page=10000):
    """Paginate API calls if needed for large date ranges."""
    all_data = []
    last_ts = None

    for page in range(max_pages):
        if page == 0:
            data = fetch_func(*args, limit=limit_per_page)
        else:
            # Modify start_ms to last timestamp + 1
            modified_args = list(args)
            modified_args[1] = last_ts + 1  # start_ms is second arg
            data = fetch_func(*modified_args, limit=limit_per_page)

        if not data or len(data) == 0:
            break

        all_data.extend(data)
        last_ts = data[-1][0]

        if len(data) < limit_per_page:
            break  # No more data

    return all_data


def collect_period(period_key):
    """Collect all data for a specific period."""
    days_back, timeframe = PERIODS[period_key]
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days_back)
    start_ms = int(start.timestamp() * 1000)

    print(f"\n{'='*50}")
    print(f"Collecting {period_key} (last {days_back} days, {timeframe} candles)")
    print(f"{'='*50}")

    result = {"updated_at": now.isoformat(), "period": period_key}

    for coin_key, symbol in COINS.items():
        print(f"\n--- {coin_key.upper()} ({symbol}) ---")

        # Fetch longs
        longs = fetch_position_data(symbol, "long", start_ms)
        print(f"  Longs: {len(longs)} data points")

        # Fetch shorts
        shorts = fetch_position_data(symbol, "short", start_ms)
        print(f"  Shorts: {len(shorts)} data points")

        # Fetch price candles
        candles = fetch_candle_data(symbol, timeframe, start_ms)
        print(f"  Candles: {len(candles)} data points")

        # Process candles -> [timestamp, close_price]
        price_data = []
        for c in candles:
            if isinstance(c, list) and len(c) >= 3:
                price_data.append([c[0], c[2]])  # [mts, close]

        result[coin_key] = {
            "longs": longs if isinstance(longs, list) else [],
            "shorts": shorts if isinstance(shorts, list) else [],
            "price": price_data,
        }

    return result


def downsample(data, max_points=2000):
    """Downsample data to max_points if too large."""
    if not data or len(data) <= max_points:
        return data
    step = len(data) / max_points
    return [data[int(i * step)] for i in range(max_points)]


def save_period(period_key, data):
    """Save period data to JSON file with downsampling."""
    for coin_key in COINS:
        if coin_key in data:
            coin = data[coin_key]
            coin["longs"] = downsample(coin["longs"], 2500)
            coin["shorts"] = downsample(coin["shorts"], 2500)
            coin["price"] = downsample(coin["price"], 2500)

    filepath = os.path.join(DATA_DIR, f"{period_key}.json")
    with open(filepath, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    size_kb = os.path.getsize(filepath) / 1024
    print(f"\nSaved {filepath} ({size_kb:.1f} KB)")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    print("Bitfinex Margin Data Collector")
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}")
    print(f"Coins: {', '.join(c.upper() for c in COINS)}")

    for period_key in PERIODS:
        data = collect_period(period_key)
        save_period(period_key, data)

    # Create a metadata file
    meta = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "coins": list(COINS.keys()),
        "periods": list(PERIODS.keys()),
        "symbols": COINS,
    }
    with open(os.path.join(DATA_DIR, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\nAll done! Updated at {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
