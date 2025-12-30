import time
from datetime import datetime
from random import uniform, randint

from backend.database import get_connection


def ingest_fake_batch(symbol: str, batch_size: int = 20):
    """
    Inserts a batch of fake market data rows into the database.
    This simulates what a real API pull would do.
    """

    conn = get_connection()
    cursor = conn.cursor()

    rows = []

    for _ in range(batch_size):
        strike = round(uniform(15000, 16000), 2)
        price = round(uniform(100, 500), 2)
        volume = randint(1, 1000)
        timestamp = datetime.utcnow().isoformat()

        rows.append((symbol, strike, price, volume, timestamp))

    cursor.executemany(
        """
        INSERT INTO market_data (symbol, strike, price, volume, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows
    )

    conn.commit()
    conn.close()


def ingestion_loop(
    symbol: str,
    interval_seconds: int = 30,
    batch_size: int = 20
):
    """
    Long-running ingestion loop.

    - Runs forever
    - Sleeps between pulls
    - Can be stopped when the app shuts down
    """

    while True:
        ingest_fake_batch(symbol, batch_size)
        time.sleep(interval_seconds)