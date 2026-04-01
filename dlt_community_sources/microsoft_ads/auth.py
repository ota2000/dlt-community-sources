"""Microsoft Advertising OAuth2 authentication helpers."""

import logging

import requests

logger = logging.getLogger(__name__)

MSFT_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"


def refresh_access_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> dict:
    """Refresh access_token using refresh_token via Microsoft Entra ID.

    Microsoft rotates refresh_token on each refresh. The caller must
    persist the new refresh_token.

    Returns:
        Dict with 'access_token', 'refresh_token', 'expires_in', etc.
    """
    response = requests.post(
        MSFT_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "scope": "https://ads.microsoft.com/msads.manage offline_access",
        },
    )
    response.raise_for_status()
    return response.json()
