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


app = FastAPI(
    title="Market Data App",
    lifespan=lifespan
)


@app.get("/health")
def health():
    return {"status": "ok"}


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
    # Default limit to 100 records
    limit: int = 100,
    strike: Optional[float] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    if strike is not None:
        cursor.execute(
            """
            SELECT symbol, strike, price, volume, timestamp
            FROM market_data
            WHERE symbol = ? AND strike = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, strike, limit)
        )
    else:
        cursor.execute(
            """
            SELECT symbol, strike, price, volume, timestamp
            FROM market_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit)
        )

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]
