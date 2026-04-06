#!/usr/bin/env python3
"""
Runs once daily after US market close.
Fetches full price history for US indexes (Stooq + Yahoo) and SPX 125-day SMA.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
STOOQ_BASE = "https://stooq.com/q/d/l/"

# Stooq has deeper history for major indexes
STOOQ_SYMBOLS = {
    "S&P 500":   "^spx",
    "Dow Jones": "^dji",
    "NASDAQ":    "^ndq",
}
# Yahoo symbol for live price override on Stooq entries
STOOQ_TO_YAHOO = {
    "^spx": "%5EGSPC",
    "^dji": "%5EDJI",
    "^ndq": "%5EIXIC",
}
# Yahoo-only indexes
YAHOO_SYMBOLS = {
    "Russell 2000": "%5ERUT",
}


def save(path: str, data: object):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) // 1024
    print(f"  Saved {path} ({size_kb} KB)")


def parse_stooq_csv(text: str) -> list:
    lines = text.strip().splitlines()
    result = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 5:
            continue
        try:
            dt = datetime.strptime(parts[0].strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
            close = float(parts[4].strip())
            result.append({"x": int(dt.timestamp() * 1000), "y": close})
        except (ValueError, IndexError):
            continue
    return sorted(result, key=lambda p: p["x"])


def fetch_live_yahoo(symbol: str) -> tuple:
    try:
        r = requests.get(
            f"{YAHOO_BASE}{symbol}",
            params={"interval": "1m", "range": "1d"},
            headers=HEADERS, timeout=15,
        )
        r.raise_for_status()
        meta = r.json()["chart"]["result"][0]["meta"]
        return (
            meta.get("regularMarketPrice"),
            meta.get("chartPreviousClose") or meta.get("previousClose"),
        )
    except Exception:
        return None, None


def calculate_sma(data: list, period: int) -> list:
    result = []
    for i in range(len(data)):
        if i < period - 1:
            continue
        window = data[i - period + 1: i + 1]
        result.append({"x": data[i]["x"], "y": sum(p["y"] for p in window) / period})
    return result


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== fetch_daily.py  {ts} ===")
    now = datetime.now(timezone.utc)
    indices = []

    stooq_start = (now - timedelta(days=6 * 365)).strftime("%Y%m%d")
    stooq_end   = now.strftime("%Y%m%d")

    for name, symbol in STOOQ_SYMBOLS.items():
        yahoo_sym = STOOQ_TO_YAHOO.get(symbol)
        history = None

        # Try Stooq first (deeper history)
        print(f"  {name} (Stooq)")
        try:
            r = requests.get(
                STOOQ_BASE,
                params={"s": symbol, "d1": stooq_start, "d2": stooq_end, "i": "d"},
                headers=HEADERS, timeout=20,
            )
            r.raise_for_status()
            parsed = parse_stooq_csv(r.text)
            if len(parsed) >= 2:
                history = parsed
        except Exception as e:
            print(f"    Stooq failed for {name}: {e}")

        # Fall back to Yahoo if Stooq returned nothing
        if not history and yahoo_sym:
            print(f"  {name} (Yahoo fallback)")
            try:
                r = requests.get(
                    f"{YAHOO_BASE}{yahoo_sym}",
                    params={"interval": "1d", "range": "5y"},
                    headers=HEADERS, timeout=20,
                )
                r.raise_for_status()
                result = r.json()["chart"]["result"][0]
                timestamps = result["timestamp"]
                closes = result["indicators"]["quote"][0]["close"]
                parsed = [
                    {"x": int(ts) * 1000, "y": round(float(c), 4)}
                    for ts, c in zip(timestamps, closes)
                    if c is not None
                ]
                if len(parsed) >= 2:
                    history = parsed
            except Exception as e:
                print(f"    Yahoo fallback failed for {name}: {e}")

        if not history:
            print(f"    SKIP {name}: no data from Stooq or Yahoo")
            time.sleep(0.4)
            continue

        price, prev_close = fetch_live_yahoo(yahoo_sym) if yahoo_sym else (None, None)
        if not price:
            price = history[-1]["y"]
            prev_close = history[-2]["y"]
        change_abs = price - prev_close
        change_pct = change_abs / prev_close * 100.0
        indices.append({
            "name": name, "price": price,
            "changePct": change_pct, "changeAbs": change_abs,
            "history": history,
        })
        time.sleep(0.4)

    for name, symbol in YAHOO_SYMBOLS.items():
        print(f"  {name} (Yahoo)")
        try:
            r = requests.get(
                f"{YAHOO_BASE}{symbol}",
                params={"interval": "1d", "range": "5y"},
                headers=HEADERS, timeout=20,
            )
            r.raise_for_status()
            result = r.json()["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]
            history = [
                {"x": int(ts) * 1000, "y": round(float(c), 4)}
                for ts, c in zip(timestamps, closes)
                if c is not None
            ]
            if len(history) < 2:
                continue
            meta = result.get("meta", {})
            price = meta.get("regularMarketPrice") or history[-1]["y"]
            prev = history[-2]["y"]
            indices.append({
                "name": name, "price": price,
                "changePct": (price - prev) / prev * 100.0,
                "changeAbs": price - prev,
                "history": history,
            })
        except Exception as e:
            print(f"    ERROR {name}: {e}")
        time.sleep(0.4)

    save("data/indexes-history.json", {
        "fetched_at": int(time.time() * 1000),
        "indices": indices,
    })

    # SPX 125-day SMA (need 3+ years of daily data for a full 1-year SMA history)
    print("SPX 125d SMA...")
    spx_points = None

    # Try Stooq first
    try:
        start = (now - timedelta(days=3 * 365)).strftime("%Y%m%d")
        r = requests.get(
            STOOQ_BASE,
            params={"s": "^spx", "d1": start, "d2": stooq_end, "i": "d"},
            headers=HEADERS, timeout=20,
        )
        r.raise_for_status()
        parsed = parse_stooq_csv(r.text)
        if len(parsed) >= 200:
            spx_points = parsed
            print("    Stooq OK")
        else:
            print(f"    Stooq returned only {len(parsed)} points, trying Yahoo...")
    except Exception as e:
        print(f"    Stooq failed: {e}, trying Yahoo...")

    # Fall back to Yahoo (5y gives plenty of warmup for 125-day SMA)
    if not spx_points:
        try:
            r = requests.get(
                f"{YAHOO_BASE}%5EGSPC",
                params={"interval": "1d", "range": "5y"},
                headers=HEADERS, timeout=20,
            )
            r.raise_for_status()
            result = r.json()["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]
            spx_points = [
                {"x": int(ts) * 1000, "y": round(float(c), 4)}
                for ts, c in zip(timestamps, closes)
                if c is not None
            ]
            print(f"    Yahoo OK ({len(spx_points)} points)")
        except Exception as e:
            print(f"    ERROR SPX SMA Yahoo: {e}")

    if spx_points and len(spx_points) >= 125:
        save("data/spx-sma.json", calculate_sma(spx_points, 125))

    print("=== Done ===")


if __name__ == "__main__":
    main()
