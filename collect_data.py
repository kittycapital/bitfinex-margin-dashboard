#!/usr/bin/env python3
"""
Bitfinex Margin Long/Short Position Data Collector
Collects BTC, ETH, SOL margin longs/shorts and price data.
Designed to run via GitHub Actions on a schedule.

Stats API uses 1h timeframe for all periods:
  - 1h × 10000 = 416 days per page
  - 90d/1y = 1 page, 3y = 3 pages, 5y = 5 pages, all = 9 pages
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

# Period configs: (days_back, candle_timeframe, max_stat_pages)
# Stats always use 1h: 10000 × 1h = 416 days per page
PERIODS = {
    "90d":  (90,    "1h",  1),
    "1y":   (365,   "4h",  1),
    "3y":   (1095,  "1D",  3),
    "5y":   (1825,  "1D",  5),
    "all":  (3650,  "1D",  9),
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


def fetch_position_paged(symbol, side, start_ms, max_pages=1):
    """
    Fetch margin position data with pagination using 1h timeframe.
    Each page: 10000 × 1h = 416 days.
    Paginates backwards from now.
    """
    all_data = []
    cursor = int(time.time() * 1000)

    for page in range(max_pages):
        url = (
            f"{BASE_URL}/stats1/pos.size:1h:{symbol}:{side}/hist"
            f"?limit=10000&start={start_ms}&end={cursor}&sort=-1"
        )
        if page == 0:
            print(f"  Fetching {symbol} {side} (1h, up to {max_pages} pages)...")

        data = fetch_json(url)
        time.sleep(RATE_LIMIT_DELAY)

        if not isinstance(data, list) or not data:
            break

        all_data.extend(data)

        # Move cursor before oldest point
        oldest_ts = data[-1][0]
        cursor = oldest_ts - 1

        if cursor <= start_ms or len(data) < 10000:
            break

        if page > 0:
            print(f"    ...page {page+1}, {len(all_data)} points so far")

    # Sort chronologically
    all_data.sort(key=lambda x: x[0])

    # Deduplicate by timestamp
    seen = set()
    deduped = []
    for item in all_data:
        ts = item[0]
        if ts not in seen:
            seen.add(ts)
            deduped.append(item)

    print(f"  {side.capitalize()}: {len(deduped)} data points")
    return deduped


def fetch_candle_data(symbol, timeframe, start_ms):
    """Fetch OHLCV candle data for price."""
    url = (
        f"{BASE_URL}/candles/trade:{timeframe}:{symbol}/hist"
        f"?limit=10000&start={start_ms}&sort=-1"
    )
    print(f"  Fetching {symbol} candles ({timeframe})...")
    data = fetch_json(url)
    time.sleep(RATE_LIMIT_DELAY)
    return data


def collect_period(period_key):
    """Collect all data for a specific period."""
    days_back, candle_tf, max_pages = PERIODS[period_key]
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days_back)
    start_ms = int(start.timestamp() * 1000)

    print(f"\n{'='*50}")
    print(f"Collecting {period_key} (last {days_back} days, candle={candle_tf}, stat_pages={max_pages})")
    print(f"{'='*50}")

    result = {"updated_at": now.isoformat(), "period": period_key}

    for coin_key, symbol in COINS.items():
        print(f"\n--- {coin_key.upper()} ({symbol}) ---")

        # Fetch longs (paginated, 1h timeframe)
        longs = fetch_position_paged(symbol, "long", start_ms, max_pages)

        # Fetch shorts (paginated, 1h timeframe)
        shorts = fetch_position_paged(symbol, "short", start_ms, max_pages)

        # Fetch price candles
        candles = fetch_candle_data(symbol, candle_tf, start_ms)
        if isinstance(candles, list):
            candles.reverse()
        print(f"  Candles: {len(candles) if isinstance(candles, list) else 0} data points")

        # Process candles -> [timestamp, close_price]
        price_data = []
        for c in candles:
            if isinstance(c, list) and len(c) >= 3:
                price_data.append([c[0], c[2]])

        result[coin_key] = {
            "longs": longs,
            "shorts": shorts,
            "price": price_data,
        }

    return result


def downsample(data, max_points=2500):
    """Downsample data to max_points if too large, keeping first and last."""
    if not data or len(data) <= max_points:
        return data
    step = (len(data) - 1) / (max_points - 1)
    result = [data[int(i * step)] for i in range(max_points - 1)]
    result.append(data[-1])
    return result


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
