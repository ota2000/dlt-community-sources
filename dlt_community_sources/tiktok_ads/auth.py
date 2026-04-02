"""TikTok Marketing API authentication helpers."""

import requests

TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
REFRESH_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token/"


def get_access_token(
    app_id: str,
    secret: str,
    auth_code: str,
) -> dict:
    """Exchange auth code for access_token and refresh_token.

    Returns:
        Dict with 'access_token' and 'refresh_token'.
    """
    response = requests.post(
        TOKEN_URL,
        json={
            "app_id": app_id,
            "secret": secret,
            "auth_code": auth_code,
        },
    )
    response.raise_for_status()
    data = response.json()
    if data.get("code") != 0:
        raise RuntimeError(
            f"TikTok auth error: {data.get('message')} (code={data.get('code')})"
        )
    return data["data"]


def refresh_access_token(
    app_id: str,
    secret: str,
    refresh_token: str,
) -> dict:
    """Refresh access_token using refresh_token.

    Returns:
        Dict with new 'access_token' and new 'refresh_token'.
    """
    response = requests.post(
        REFRESH_URL,
        json={
            "app_id": app_id,
            "secret": secret,
            "refresh_token": refresh_token,
        },
    )
    response.raise_for_status()
    data = response.json()
    if data.get("code") != 0:
        raise RuntimeError(
            f"TikTok refresh error: {data.get('message')} (code={data.get('code')})"
        )
    return data["data"]
