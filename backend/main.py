from fastapi import FastAPI
from contextlib import asynccontextmanager
from datetime import datetime
from random import uniform, randint
from typing import Optional, List
import math
import threading
from backend.ingestion import fetch_schwab_option_chain

from backend.database import init_db, get_connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- STARTUP ----
    init_db()

    # App runs here
    yield

    # ---- SHUTDOWN ----
    # Nothing to clean up yet

# Create FastAPI app with lifespan
app = FastAPI(
    title="Market Data App",
    lifespan=lifespan
)

# Health check endpoint
@app.get("/health")
def health():
    return {"status": "ok"}


# Endpoint to ingest fake market data
@app.post("/ingest/fake")
def ingest_fake_data(symbol: str = "NQ"):
    conn = get_connection()
    cursor = conn.cursor()

    strike = round(uniform(15000, 16000), 2)
    price = round(uniform(100, 500), 2)
    volume = randint(1, 1000)
    timestamp = datetime.utcnow().isoformat()

    cursor.execute(
        """
        INSERT INTO market_data (symbol, strike, price, volume, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """,
        (symbol, strike, price, volume, timestamp)
    )

    conn.commit()
    conn.close()

    return {
        "symbol": symbol,
        "strike": strike,
        "price": price,
        "volume": volume,
        "timestamp": timestamp
    }


@app.get("/data")
def read_data(
    symbol: str,
    limit: int = 100,
    strike: Optional[float] = None,
    start: Optional[str] = None,
    end: Optional[str] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    # Base query
    query = """
        SELECT symbol, strike, price, volume, timestamp
        FROM market_data
        WHERE symbol = ?
    """
    params = [symbol]

    # Optional strike filter
    if strike is not None:
        query += " AND strike = ?"
        params.append(strike)

    # Optional start time filter
    if start is not None:
        query += " AND timestamp >= ?"
        params.append(start)

    # Optional end time filter
    if end is not None:
        query += " AND timestamp <= ?"
        params.append(end)

    # Sorting + limit
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


@app.get("/volume-by-strike")
def volume_by_strike(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            strike,
            SUM(volume) AS total_volume
        FROM market_data
        WHERE symbol = ?
    """
    params = [symbol]

    if start is not None:
        query += " AND timestamp >= ?"
        params.append(start)

    if end is not None:
        query += " AND timestamp <= ?"
        params.append(end)

    query += """
        GROUP BY strike
        ORDER BY total_volume DESC
    """

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


@app.get("/volume-by-strike-bucketed")
def volume_by_strike_bucketed(
    symbol: str,
    bucket_size: float = 5.0,
    start: Optional[str] = None,
    end: Optional[str] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    query = f"""
        SELECT
            CAST(FLOOR(strike / ?) * ? AS REAL) AS strike_bucket,
            SUM(volume) AS total_volume
        FROM market_data
        WHERE symbol = ?
    """
    params = [bucket_size, bucket_size, symbol]

    if start is not None:
        query += " AND timestamp >= ?"
        params.append(start)

    if end is not None:
        query += " AND timestamp <= ?"
        params.append(end)

    query += """
        GROUP BY strike_bucket
        ORDER BY strike_bucket
    """

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


@app.get("/poc")
def point_of_control(
    symbol: str,
    bucket_size: float = 5.0,
    start: Optional[str] = None,
    end: Optional[str] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            CAST(FLOOR(strike / ?) * ? AS REAL) AS strike_bucket,
            SUM(volume) AS total_volume
        FROM market_data
        WHERE symbol = ?
    """
    params = [bucket_size, bucket_size, symbol]

    if start is not None:
        query += " AND timestamp >= ?"
        params.append(start)

    if end is not None:
        query += " AND timestamp <= ?"
        params.append(end)

    query += """
        GROUP BY strike_bucket
        ORDER BY total_volume DESC
        LIMIT 1
    """

    cursor.execute(query, params)
    row = cursor.fetchone()
    conn.close()

    # If no data exists for the query
    if row is None:
        return {"poc": None, "total_volume": 0}

    return {
        "poc": row["strike_bucket"],
        "total_volume": row["total_volume"]
    }


@app.get("/value-area")
def value_area(
    symbol: str,
    bucket_size: float = 5.0,
    value_area_pct: float = 0.70,
    start: Optional[str] = None,
    end: Optional[str] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    # Step 1: get bucketed volume profile
    query = """
        SELECT
            CAST(FLOOR(strike / ?) * ? AS REAL) AS strike_bucket,
            SUM(volume) AS total_volume
        FROM market_data
        WHERE symbol = ?
    """
    params = [bucket_size, bucket_size, symbol]

    if start is not None:
        query += " AND timestamp >= ?"
        params.append(start)

    if end is not None:
        query += " AND timestamp <= ?"
        params.append(end)

    query += """
        GROUP BY strike_bucket
        ORDER BY strike_bucket
    """

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    # No data case
    if not rows:
        return {
            "poc": None,
            "vah": None,
            "val": None,
            "total_volume": 0
        }

    # Convert rows to a list of dicts
    profile = [dict(row) for row in rows]

    # Step 2: total volume and target value area volume
    total_volume = sum(r["total_volume"] for r in profile)
    target_volume = total_volume * value_area_pct

    # Step 3: find POC (max volume bucket)
    poc_row = max(profile, key=lambda r: r["total_volume"])
    poc_index = profile.index(poc_row)

    # Step 4: expand from POC
    cumulative_volume = poc_row["total_volume"]
    included_indices = {poc_index}

    left = poc_index - 1
    right = poc_index + 1

    while cumulative_volume < target_volume and (left >= 0 or right < len(profile)):
        left_volume = profile[left]["total_volume"] if left >= 0 else -1
        right_volume = profile[right]["total_volume"] if right < len(profile) else -1

        if right_volume >= left_volume:
            included_indices.add(right)
            cumulative_volume += right_volume
            right += 1
        else:
            included_indices.add(left)
            cumulative_volume += left_volume
            left -= 1

    # Step 5: determine VAH / VAL
    included_buckets = [profile[i]["strike_bucket"] for i in included_indices]

    return {
        "poc": poc_row["strike_bucket"],
        "vah": max(included_buckets),
        "val": min(included_buckets),
        "total_volume": total_volume,
        "value_area_volume": cumulative_volume,
        "value_area_pct": value_area_pct
    }


@app.get("/cumulative-volume-profile")
def cumulative_volume_profile(
    symbol: str,
    bucket_size: float = 5.0,
    start: Optional[str] = None,
    end: Optional[str] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    # Step 1: bucketed volume profile
    query = """
        SELECT
            CAST(FLOOR(strike / ?) * ? AS REAL) AS strike_bucket,
            SUM(volume) AS total_volume
        FROM market_data
        WHERE symbol = ?
    """
    params = [bucket_size, bucket_size, symbol]

    if start is not None:
        query += " AND timestamp >= ?"
        params.append(start)

    if end is not None:
        query += " AND timestamp <= ?"
        params.append(end)

    query += """
        GROUP BY strike_bucket
        ORDER BY strike_bucket
    """

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    profile = [dict(row) for row in rows]

    # Step 2: cumulative volume calculation
    cumulative = 0
    result = []

    for row in profile:
        cumulative += row["total_volume"]
        result.append({
            "strike": row["strike_bucket"],
            "volume": row["total_volume"],
            "cumulative_volume": cumulative
        })

    return result


@app.get("/test/schwab")
def test_schwab(symbol: str = "SPY"):
    data = fetch_schwab_option_chain(symbol)

    return {
        "symbol": data.get("symbol"),
        "underlyingPrice": data.get("underlyingPrice"),
        "status": "ok"
    }



