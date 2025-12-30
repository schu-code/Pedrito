from fastapi import FastAPI
from contextlib import asynccontextmanager
from datetime import datetime
from random import uniform, randint
from typing import Optional, List
import math

from backend.database import init_db, get_connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- startup ----
    init_db()
    yield
    # ---- shutdown ----
    # (nothing to clean up yet)

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