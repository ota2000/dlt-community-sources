"""JWT authentication for App Store Connect API."""

import time

import jwt


def generate_token(
    key_id: str,
    issuer_id: str,
    private_key: str,
    expiration_seconds: int = 1140,  # 19 minutes (max 20)
) -> str:
    """Generate a JWT token for App Store Connect API.

    Args:
        key_id: API key ID from App Store Connect.
        issuer_id: Issuer ID from App Store Connect.
        private_key: Contents of the .p8 private key file.
        expiration_seconds: Token expiration in seconds. Max 1200 (20 min).

    Returns:
        JWT token string.
    """
    now = int(time.time())
    payload = {
        "iss": issuer_id,
        "iat": now,
        "exp": now + expiration_seconds,
        "aud": "appstoreconnect-v1",
    }
    headers = {
        "alg": "ES256",
        "kid": key_id,
        "typ": "JWT",
    }
    return jwt.encode(payload, private_key, algorithm="ES256", headers=headers)


class AppStoreConnectAuth:
    """Authentication for App Store Connect API.

    Generates JWT tokens on each request to avoid expiration issues.
    Compatible with both dlt rest_api source and requests.Session.
    """

    def __init__(self, key_id: str, issuer_id: str, private_key: str) -> None:
        self.key_id = key_id
        self.issuer_id = issuer_id
        self.private_key = private_key

    def __call__(self, request):
        """Apply auth to a PreparedRequest (called by requests per-request)."""
        token = generate_token(self.key_id, self.issuer_id, self.private_key)
        request.headers["Authorization"] = f"Bearer {token}"
        return request
