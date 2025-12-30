from fastapi import FastAPI
from contextlib import asynccontextmanager
from datetime import datetime
from random import uniform, randint
from typing import Optional, List

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
