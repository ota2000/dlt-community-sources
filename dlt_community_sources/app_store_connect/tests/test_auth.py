"""Tests for JWT authentication."""

import jwt as pyjwt

from dlt_community_sources.app_store_connect.auth import generate_token

# Test key generated for testing only - not a real key
TEST_PRIVATE_KEY = """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEILhwBLXcPIjna02ld7Ifk8poVFmhbD5gGIQfuanlitFnoAoGCCqGSM49
AwEHoUQDQgAEWDusakKGGVeANoNlC2U4QdOst3IkbxoIdq736rFAP9x9IpyR+Gs7
oP4O5IpOPmrqV/5E47OntNnClkTQ+GZWIw==
-----END EC PRIVATE KEY-----"""


def test_generate_token():
    token = generate_token(
        key_id="TEST_KEY_ID",
        issuer_id="TEST_ISSUER_ID",
        private_key=TEST_PRIVATE_KEY,
    )
    assert isinstance(token, str)
    assert len(token) > 0

    # Decode without verification to check claims
    decoded = pyjwt.decode(token, options={"verify_signature": False})
    assert decoded["iss"] == "TEST_ISSUER_ID"
    assert decoded["aud"] == "appstoreconnect-v1"
    assert "iat" in decoded
    assert "exp" in decoded


def test_token_expiration():
    token = generate_token(
        key_id="TEST_KEY_ID",
        issuer_id="TEST_ISSUER_ID",
        private_key=TEST_PRIVATE_KEY,
        expiration_seconds=600,
    )
    decoded = pyjwt.decode(token, options={"verify_signature": False})
    assert decoded["exp"] - decoded["iat"] == 600


def test_token_headers():
    token = generate_token(
        key_id="MY_KEY_ID",
        issuer_id="TEST_ISSUER_ID",
        private_key=TEST_PRIVATE_KEY,
    )
    headers = pyjwt.get_unverified_header(token)
    assert headers["alg"] == "ES256"
    assert headers["kid"] == "MY_KEY_ID"
    assert headers["typ"] == "JWT"
