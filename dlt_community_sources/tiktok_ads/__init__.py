"""TikTok Marketing API source for dlt."""

from .auth import refresh_access_token
from .source import tiktok_ads_source

__all__ = ["tiktok_ads_source", "refresh_access_token"]
