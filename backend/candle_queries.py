
from backend.database import get_connection


def get_candles(
    symbol: str,
    timeframe: str,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
):
    """
    Fetch candles from SQLite ordered by datetime ascending.
    Datetimes are ISO strings (UTC).
    """

    query = """
        SELECT
            symbol,
            datetime,
            open,
            high,
            low,
            close,
            volume,
            timeframe
        FROM candles
        WHERE symbol = ?
          AND timeframe = ?
    """

    params = [symbol, timeframe]

    if start_datetime:
        query += " AND datetime >= ?"
        params.append(start_datetime)

    if end_datetime:
        query += " AND datetime <= ?"
        params.append(end_datetime)

    query += " ORDER BY datetime ASC"

    with get_connection() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(query, params).fetchall()

    # Convert sqlite rows → normal dicts
    return [dict(row) for row in rows]