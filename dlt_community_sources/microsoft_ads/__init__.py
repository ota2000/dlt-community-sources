"""Microsoft Advertising source for dlt."""

from .auth import refresh_access_token, refresh_access_token_with_certificate
from .source import discover_accounts, microsoft_ads_source

__all__ = [
    "microsoft_ads_source",
    "refresh_access_token",
    "refresh_access_token_with_certificate",
    "discover_accounts",
]
