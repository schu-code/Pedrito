# backend/schwab_api.py

import os
import datetime
from schwab import auth


# Read secrets from environment variables
SCHWAB_CLIENT_ID = os.environ.get("SCHWAB_APP_KEY")
SCHWAB_SECRET = os.environ.get("SCHWAB_SECRET")
SCHWAB_TOKEN_PATH = os.environ.get("SCHWAB_TOKEN_PATH")


def get_option_chain(symbol: str) -> dict:
    if not SCHWAB_CLIENT_ID or not SCHWAB_SECRET or not SCHWAB_TOKEN_PATH:
        raise RuntimeError("Missing Schwab credentials")

    schwab_client = auth.client_from_token_file(
        SCHWAB_TOKEN_PATH,
        SCHWAB_CLIENT_ID,
        SCHWAB_SECRET
    )

    today = datetime.date.today()

    response = schwab_client.get_option_chain(
        symbol,
        contract_type=schwab_client.Options.ContractType.ALL,
        include_underlying_quote=True,
        strike_count=10,
        from_date=today,
        to_date=today
    )

    if response.status_code != 200:
        raise RuntimeError(f"Schwab API error {response.status_code}")

    return response.json()


def get_option_chain_today(symbol: str) -> dict:
    """
    Pull the FULL raw option chain JSON from Schwab for today's date.
    No parsing, no filtering.
    """

    if not SCHWAB_CLIENT_ID or not SCHWAB_SECRET or not SCHWAB_TOKEN_PATH:
        raise RuntimeError("Missing Schwab credentials")

    schwab_client = auth.client_from_token_file(
        SCHWAB_TOKEN_PATH,
        SCHWAB_CLIENT_ID,
        SCHWAB_SECRET
    )

    # ---- date logic exactly as requested ----
    now = datetime.datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    # ----------------------------------------

    response = schwab_client.get_option_chain(
        symbol,
        contract_type=schwab_client.Options.ContractType.ALL,
        include_underlying_quote=True,
        from_date=date,
        to_date=date
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Schwab API error {response.status_code}: {response.text}"
        )

    return response.json()


def get_quotes(symbols: list[str]) -> dict:
    """
    Fetch full quote data for one or more symbols.
    Returns raw Schwab quote JSON.
    """

    if not SCHWAB_CLIENT_ID or not SCHWAB_SECRET or not SCHWAB_TOKEN_PATH:
        raise RuntimeError("Missing Schwab credentials")

    schwab_client = auth.client_from_token_file(
        SCHWAB_TOKEN_PATH,
        SCHWAB_CLIENT_ID,
        SCHWAB_SECRET
    )

    response = schwab_client.get_quotes(
        symbols=symbols
        # fields=None → returns ALL available fields
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Schwab API error {response.status_code}: {response.text}"
        )

    return response.json()
