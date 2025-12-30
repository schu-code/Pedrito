# backend/schwab_api.py

import os
import datetime
from schwab import auth, client

TOKEN_PATH = os.environ.get("SCHWAB_TOKEN_PATH")
API_KEY = os.environ.get("SCHWAB_API_KEY")


def get_option_chain(symbol: str) -> dict:
    if not TOKEN_PATH or not API_KEY:
        raise RuntimeError("Missing Schwab credentials in environment variables")

    schwab_client = auth.client_from_token_file(
        TOKEN_PATH,
        API_KEY
    )

    today = datetime.date.today()

    response = schwab_client.get_option_chain(
        symbol,
        contract_type=client.Options.ContractType.ALL,
        from_date=today,
        to_date=today
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Schwab API error {response.status_code}: {response.text}"
        )

    return response.json()