import schwab
import os

CLIENT_ID = os.environ.get("SCHWAB_APP_KEY")
CLIENT_SECRET = os.environ.get("SCHWAB_SECRET")
REDIRECT_URI = "https://127.0.0.1"
TOKEN_PATH = "token.json"

def manual_auth():
    schwab.auth.client_from_manual_flow(
        CLIENT_ID,
        CLIENT_SECRET,
        REDIRECT_URI,
        TOKEN_PATH,
        asyncio=False
    )

if __name__ == "__main__":
    manual_auth()