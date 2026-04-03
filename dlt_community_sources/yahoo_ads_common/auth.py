"""Yahoo Ads OAuth2 authentication helpers.

Shared by both Search Ads (SS) and Display Ads (YDA).
Token endpoint: https://biz-oauth.yahoo.co.jp/oauth/v1/token
"""

import logging

import requests

logger = logging.getLogger(__name__)

YAHOO_TOKEN_URL = "https://biz-oauth.yahoo.co.jp/oauth/v1/token"


def refresh_access_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> dict:
    """Refresh access_token using refresh_token.

    Yahoo Ads does NOT rotate refresh_token on each refresh.
    The refresh_token remains valid as long as it's used within 4 weeks.

    Returns:
        Dict with 'access_token', 'token_type', 'expires_in', etc.
    """
    response = requests.post(
        YAHOO_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()
