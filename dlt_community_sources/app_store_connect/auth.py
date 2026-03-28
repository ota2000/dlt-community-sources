"""JWT authentication for App Store Connect API."""

import time

import jwt
from dlt.common.configuration import configspec
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from requests import PreparedRequest


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


@configspec
class AppStoreConnectAuth(BearerTokenAuth):
    """Authentication for App Store Connect API.

    Extends BearerTokenAuth to regenerate JWT on each request.
    """

    key_id: str = None
    issuer_id: str = None
    private_key: str = None

    def __init__(self, key_id: str, issuer_id: str, private_key: str) -> None:
        self.key_id = key_id
        self.issuer_id = issuer_id
        self.private_key = private_key
        super().__init__(token=generate_token(key_id, issuer_id, private_key))

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Regenerate JWT token on each request to avoid expiration."""
        self.token = generate_token(self.key_id, self.issuer_id, self.private_key)
        return super().__call__(request)
