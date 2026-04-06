"""Microsoft Advertising OAuth2 authentication helpers."""

import logging

import requests

logger = logging.getLogger(__name__)

MSFT_TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"


def refresh_access_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    tenant_id: str = "common",
) -> dict:
    """Refresh access_token using refresh_token via Microsoft Entra ID.

    Microsoft rotates refresh_token on each refresh. The caller must
    persist the new refresh_token.

    Args:
        client_id: Azure App Registration client ID.
        client_secret: Azure App Registration client secret.
        refresh_token: Current refresh token.
        tenant_id: Azure AD tenant ID. Use 'common' for multi-tenant apps,
            or a specific tenant ID for single-tenant apps.

    Returns:
        Dict with 'access_token', 'refresh_token', 'expires_in', etc.
    """
    token_url = MSFT_TOKEN_URL_TEMPLATE.format(tenant=tenant_id)
    response = requests.post(
        token_url,
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "scope": "https://ads.microsoft.com/msads.manage offline_access",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()
