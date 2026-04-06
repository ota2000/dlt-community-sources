"""Microsoft Advertising OAuth2 authentication helpers."""

import logging

import requests

logger = logging.getLogger(__name__)

MSFT_TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
MSFT_SCOPE = "https://ads.microsoft.com/msads.manage offline_access"


def refresh_access_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    tenant_id: str = "common",
) -> dict:
    """Refresh access_token using client_secret + refresh_token.

    Microsoft rotates refresh_token on each refresh. The caller must
    persist the new refresh_token (e.g., to Secret Manager).

    Args:
        client_id: Azure App Registration client ID.
        client_secret: Azure App Registration client secret.
        refresh_token: Current refresh token.
        tenant_id: Azure AD tenant ID.

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
            "scope": MSFT_SCOPE,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def refresh_access_token_with_certificate(
    client_id: str,
    tenant_id: str,
    refresh_token: str,
    private_key: str,
    thumbprint: str,
) -> dict:
    """Refresh access_token using certificate + refresh_token.

    Uses certificate-based authentication instead of client_secret.
    Certificates can have much longer validity (up to 100 years),
    eliminating the need for periodic client_secret rotation.

    Microsoft rotates refresh_token on each refresh. The caller must
    persist the new refresh_token.

    Args:
        client_id: Azure App Registration client ID.
        tenant_id: Azure AD tenant ID (required, not 'common').
        refresh_token: Current refresh token.
        private_key: PEM-encoded private key string.
        thumbprint: Certificate thumbprint (SHA-1 hex).

    Returns:
        Dict with 'access_token', 'refresh_token', 'expires_in', etc.
    """
    import msal

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential={"thumbprint": thumbprint, "private_key": private_key},
    )
    result = app.acquire_token_by_refresh_token(
        refresh_token,
        scopes=["https://ads.microsoft.com/msads.manage"],
    )
    if "error" in result:
        raise RuntimeError(
            f"Certificate auth failed: {result.get('error_description', result['error'])}"
        )
    return result
