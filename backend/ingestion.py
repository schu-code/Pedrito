# backend/ingestion.py

from backend.schwab_api import get_option_chain


def fetch_schwab_option_chain(symbol: str):
    """
    Temporary ingestion function used ONLY to test the Schwab API pull.
    """

    return get_option_chain(symbol)
