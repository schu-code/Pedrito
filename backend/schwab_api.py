# backend/schwab_api.py

import os
import datetime
from schwab import auth


# Read secrets from environment variables
SCHWAB_CLIENT_ID = os.environ.get("SCHWAB_APP_KEY")
SCHWAB_SECRET = os.environ.get("SCHWAB_SECRET")
SCHWAB_TOKEN_PATH = os.environ.get("SCHWAB_TOKEN_PATH")


def get_option_chain(symbol: str) -> dict:
    """
    Fetch raw option chain data from Schwab and return JSON.
    """

    # Explicit safety check
    if not SCHWAB_CLIENT_ID or not SCHWAB_SECRET or not SCHWAB_TOKEN_PATH:
        raise RuntimeError(
            "Missing Schwab credentials. "
            "Ensure SCHWAB_API_KEY, SCHWAB_CLIENT_SECRET, "
            "and SCHWAB_TOKEN_PATH are set."
        )

    # Authenticate using cached token.json
    schwab_client = auth.client_from_token_file(
        SCHWAB_TOKEN_PATH,
        SCHWAB_CLIENT_ID,
        SCHWAB_SECRET
    )

    today = datetime.date.today()

    # Schwab returns calls + puts by default
    response = schwab_client.get_option_chain(
        symbol,
        from_date=today,
        to_date=today
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Schwab API error {response.status_code}: {response.text}"
        )

    return response.json()
