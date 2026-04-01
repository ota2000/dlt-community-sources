"""Tests for TikTok Ads auth helpers."""

from dlt_community_sources.tiktok_ads.auth import REFRESH_URL, TOKEN_URL


def test_token_url():
    assert "oauth2/access_token" in TOKEN_URL


def test_refresh_url():
    assert "oauth2/refresh_token" in REFRESH_URL
