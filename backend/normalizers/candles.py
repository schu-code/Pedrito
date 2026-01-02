# backend/normalizers/candles.py

import datetime

# raw = get_price_history_raw("SPY")
# candles = normalize_price_history("SPY", raw, timeframe="1m")



def normalize_price_history(symbol: str, raw_history: dict, timeframe: str):
    """
    Convert Schwab price history JSON into a list of normalized candle dicts.
    """

    candles = raw_history.get("candles", [])
    normalized = []

    for c in candles:
        # Schwab datetime is epoch milliseconds
        dt = datetime.datetime.utcfromtimestamp(c["datetime"] / 1000)

        normalized.append({
            "symbol": symbol,
            "datetime": dt.isoformat() + "Z",
            "open": c.get("open"),
            "high": c.get("high"),
            "low": c.get("low"),
            "close": c.get("close"),
            "volume": c.get("volume"),
            "timeframe": timeframe
        })

    return normalized