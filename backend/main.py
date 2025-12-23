from fastapi import FastAPI
from contextlib import asynccontextmanager
from datetime import datetime
from random import uniform, randint

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