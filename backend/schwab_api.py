# backend/schwab_api.py

import os
import datetime
from schwab import auth, client


# Read secrets from environment variables
SCHWAB_API_KEY = os.environ.get("SCHWAB_API_KEY")
SCHWAB_TOKEN_PATH = os.environ.get("SCHWAB_TOKEN_PATH")

if not SCHWAB_API_KEY or not SCHWAB_TOKEN_PATH:
    raise RuntimeError("Missing Schwab credentials in environment variables")


def get_option_chain(symbol: str) -> dict:
    """
    Fetch raw option chain data from Schwab and return JSON.
    """

    # Safety check so failures are explicit
    if not SCHWAB_API_KEY or not SCHWAB_TOKEN_PATH:
        raise RuntimeError("Missing Schwab credentials in environment variables")

    # Authenticate using cached token.json
    schwab_client = auth.client_from_token_file(
        SCHWAB_TOKEN_PATH,
        SCHWAB_API_KEY
    )

    today = datetime.date.today()

    # Call Schwab option chain endpoint
    response = schwab_client.get_option_chain(
        symbol,
        contract_type=client.Options.ContractType.ALL,
        from_date=today,
        to_date=today
    )

    # Fail loudly if something goes wrong
    if response.status_code != 200:
        raise RuntimeError(
            f"Schwab API error {response.status_code}: {response.text}"
        )

    return response.json()