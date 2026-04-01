"""Tests for TikTok Ads auth helpers."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_community_sources.tiktok_ads.auth import (
    REFRESH_URL,
    TOKEN_URL,
    get_access_token,
    refresh_access_token,
)


def test_token_url():
    assert "oauth2/access_token" in TOKEN_URL


def test_refresh_url():
    assert "oauth2/refresh_token" in REFRESH_URL


def _mock_response(json_data):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.raise_for_status.return_value = None
    return mock


class TestGetAccessToken:
    @patch("dlt_community_sources.tiktok_ads.auth.requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = _mock_response(
            {"code": 0, "data": {"access_token": "at", "refresh_token": "rt"}}
        )
        result = get_access_token("app", "secret", "code")
        assert result == {"access_token": "at", "refresh_token": "rt"}
        mock_post.assert_called_once_with(
            TOKEN_URL,
            json={"app_id": "app", "secret": "secret", "auth_code": "code"},
        )

    @patch("dlt_community_sources.tiktok_ads.auth.requests.post")
    def test_error_code(self, mock_post):
        mock_post.return_value = _mock_response(
            {"code": 40001, "message": "invalid auth_code"}
        )
        with pytest.raises(RuntimeError, match="TikTok auth error"):
            get_access_token("app", "secret", "bad_code")


class TestRefreshAccessToken:
    @patch("dlt_community_sources.tiktok_ads.auth.requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = _mock_response(
            {
                "code": 0,
                "data": {"access_token": "new_at", "refresh_token": "new_rt"},
            }
        )
        result = refresh_access_token("app", "secret", "old_rt")
        assert result == {"access_token": "new_at", "refresh_token": "new_rt"}
        mock_post.assert_called_once_with(
            REFRESH_URL,
            json={"app_id": "app", "secret": "secret", "refresh_token": "old_rt"},
        )

    @patch("dlt_community_sources.tiktok_ads.auth.requests.post")
    def test_error_code(self, mock_post):
        mock_post.return_value = _mock_response(
            {"code": 40002, "message": "invalid refresh_token"}
        )
        with pytest.raises(RuntimeError, match="TikTok refresh error"):
            refresh_access_token("app", "secret", "bad_rt")
